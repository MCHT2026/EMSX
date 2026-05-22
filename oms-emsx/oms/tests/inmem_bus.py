"""In-memory EventBus used by unit tests.

Not for production. Models Redis Streams' XADD / XREADGROUP / XACK semantics
just well enough for the unit tests in this repo:

- Each topic is an ordered list of (id, envelope) tuples.
- Each (topic, group) has its own delivered-set and PEL.
- ``publish`` fans messages out to every registered subscriber whose
  topic pattern matches.
- Idempotency keys are tracked in an in-memory dict so BaseModule's
  duplicate-check works without Redis.
- Wildcard subscriptions (``fills.*``) are evaluated lazily: every publish
  walks the subscriber list and runs ``fnmatch`` against the topic.
"""
from __future__ import annotations

import asyncio
import fnmatch
import time
import uuid
from typing import Any, Awaitable, Callable

from core.event_bus import EventBus, MessageHandler


class InMemoryBus(EventBus):
    def __init__(self) -> None:
        # topic -> list[(id, envelope)]
        self.streams: dict[str, list[tuple[str, dict]]] = {}
        # (topic, group) -> set of pending message ids
        self.pel: dict[tuple[str, str], set[str]] = {}
        # idempotency keys: key -> expiry epoch (seconds)
        self.idempotency: dict[str, float] = {}
        # (pattern, group, consumer) -> handler (one per triple)
        self._subscribers: list[tuple[str, str, str, MessageHandler, asyncio.Task]] = []
        self._connected = False
        self._counter = 0
        self._lock = asyncio.Lock()

    # ---- lifecycle --------------------------------------------------------

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False
        for _, _, _, _, task in self._subscribers:
            task.cancel()
        for _, _, _, _, task in self._subscribers:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._subscribers.clear()

    # ---- pub/sub ----------------------------------------------------------

    def _next_id(self) -> str:
        self._counter += 1
        return f"{int(time.time() * 1000)}-{self._counter}"

    async def publish(self, topic: str, payload: dict) -> str:
        msg_id = self._next_id()
        async with self._lock:
            self.streams.setdefault(topic, []).append((msg_id, payload))
            for pattern, group, _consumer, _h, _task in self._subscribers:
                if fnmatch.fnmatchcase(topic, pattern):
                    self.pel.setdefault((topic, group), set()).add(msg_id)
        # Deliver out-of-lock so handlers can call publish/ack without deadlocking.
        for pattern, group, consumer, handler, _task in list(self._subscribers):
            if fnmatch.fnmatchcase(topic, pattern):
                asyncio.create_task(self._deliver(topic, group, msg_id, payload, handler))
        return msg_id

    async def _deliver(
        self,
        topic: str,
        group: str,
        msg_id: str,
        payload: dict,
        handler: MessageHandler,
    ) -> None:
        await handler(payload, msg_id)

    async def subscribe(
        self,
        topic_pattern: str,
        group: str,
        consumer: str,
        handler: MessageHandler,
    ) -> None:
        # In-memory bus has no background poll loop — delivery is push-based
        # from publish(). The dummy task lets disconnect() cancel uniformly.
        async def _idle() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                pass
        task = asyncio.create_task(_idle())
        self._subscribers.append((topic_pattern, group, consumer, handler, task))

    async def ack(self, topic: str, group: str, message_id: str) -> None:
        pel = self.pel.get((topic, group))
        if pel is not None:
            pel.discard(message_id)

    async def get_pending(self, topic: str, group: str) -> list:
        return sorted(self.pel.get((topic, group), set()))

    async def replay_pending(
        self,
        topic: str,
        group: str,
        consumer: str,
        handler: MessageHandler,
    ) -> None:
        pending = list(self.pel.get((topic, group), set()))
        stream = dict(self.streams.get(topic, []))
        for msg_id in sorted(pending):
            if msg_id in stream:
                await handler(stream[msg_id], msg_id)

    # ---- idempotency (mimics Redis SET NX) -------------------------------

    async def set_nx(self, key: str, ttl_s: int) -> bool:
        now = time.time()
        # purge expired
        if self.idempotency.get(key, 0) < now:
            self.idempotency.pop(key, None)
        if key in self.idempotency:
            return False
        self.idempotency[key] = now + ttl_s
        return True
