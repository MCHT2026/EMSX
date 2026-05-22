"""BaseModule — the abstract base every OMS component inherits from.

Provides the four cross-cutting concerns required by the architecture:

1. **Envelope stamping.** ``publish()`` injects ``owner_id``, ``message_id``
   (UUID4), and ``timestamp`` (UTC ISO8601) on every outbound message. The
   caller cannot override these — any matching keys in *payload* survive
   under ``data`` but never overwrite the envelope.
2. **Idempotent processing.** ``is_duplicate()`` uses Redis-style ``SET NX``
   on key ``processed:{module}:{message_id}`` with a 15-minute TTL. Backed
   by the bus's own ``set_nx`` if it exposes one (in-memory bus), otherwise
   by a Redis client on the bus.
3. **At-least-once delivery.** ``process_message()`` wraps user handlers:
   check duplicate, run handler, ``XACK`` — even duplicates are acked so
   they don't sit in the PEL forever.
4. **Heartbeats.** A background task publishes ``health.heartbeat`` every
   ``HEARTBEAT_INTERVAL_S`` seconds with this module's ``pid``.

PEL replay is delegated to the bus on ``on_start()`` — replay all pending
entries this consumer owns before consuming new ones.
"""
from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from config import settings
from core.event_bus import EventBus, MessageHandler

log = logging.getLogger(__name__)

UserHandler = Callable[[dict[str, Any], str], Awaitable[None]]


def _utcnow_iso() -> str:
    """UTC ISO8601 with microseconds and 'Z' suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class BaseModule(ABC):
    """Abstract base for all OMS processes."""

    def __init__(self, name: str, bus: EventBus) -> None:
        if not name:
            raise ValueError("BaseModule.name must be non-empty")
        self.name = name
        self.bus = bus
        self._heartbeat_task: asyncio.Task | None = None
        self._stopping = asyncio.Event()
        # (topic, group, consumer, handler) tuples we're subscribed to; used
        # for PEL replay on start.
        self._subscriptions: list[tuple[str, str, str, UserHandler]] = []

    # ---- identity --------------------------------------------------------

    @property
    def consumer_name(self) -> str:
        """Consumer identity within this module's consumer group."""
        return f"{self.name}:{socket.gethostname()}"

    # ---- publish ---------------------------------------------------------

    async def publish(self, topic: str, payload: dict) -> str:
        """Publish *payload* on *topic*.

        Always injects fresh ``owner_id`` / ``message_id`` / ``timestamp`` —
        any matching keys inside *payload* survive under ``data`` but never
        overwrite the envelope.
        """
        envelope = {
            "owner_id":   self.name,
            "message_id": str(uuid.uuid4()),
            "timestamp":  _utcnow_iso(),
            "topic":      topic,
            "data":       payload,
        }
        return await self.bus.publish(topic, envelope)

    # ---- subscribe -------------------------------------------------------

    async def subscribe(self, topic_pattern: str, handler: UserHandler) -> None:
        """Subscribe this module's consumer group to *topic_pattern*.

        The wrapped handler runs through ``process_message`` so idempotency
        and ack are handled centrally.
        """

        async def _wrapped(envelope: dict, msg_id: str) -> None:
            # Resolve concrete topic — for wildcard subscriptions the envelope
            # carries the actual topic.
            topic = envelope.get("topic", topic_pattern)
            await self.process_message(topic, envelope, msg_id, handler)

        await self.bus.subscribe(
            topic_pattern,
            group=self.name,
            consumer=self.consumer_name,
            handler=_wrapped,
        )
        self._subscriptions.append(
            (topic_pattern, self.name, self.consumer_name, handler)
        )

    # ---- ack & idempotency ----------------------------------------------

    async def ack(self, topic: str, message_id: str) -> None:
        await self.bus.ack(topic, group=self.name, message_id=message_id)

    async def is_duplicate(self, message_id: str) -> bool:
        """True iff this message_id has been processed by THIS module within
        the idempotency TTL window. Implemented via Redis SET NX semantics —
        first caller wins, returns False; subsequent callers see the key
        already set and return True."""
        key = f"processed:{self.name}:{message_id}"
        ttl = settings.IDEMPOTENCY_TTL_S

        # Prefer the bus's own set_nx (e.g. in-memory tests). Falls back to
        # a redis client exposed as `bus.redis` (RedisBus does this).
        if hasattr(self.bus, "set_nx"):
            ok = await self.bus.set_nx(key, ttl)
        elif hasattr(self.bus, "redis"):
            ok = await self.bus.redis.set(key, "1", nx=True, ex=ttl)
            ok = bool(ok)
        else:
            raise RuntimeError(
                "Bus does not expose idempotency primitive (set_nx/redis)"
            )
        return not ok

    # ---- process_message -------------------------------------------------

    async def process_message(
        self,
        topic: str,
        envelope: dict,
        msg_id: str,
        handler: UserHandler,
    ) -> None:
        """Wrap a user handler with duplicate-check + ack.

        Even duplicates are acked — leaving them in the PEL would cause
        repeated replays.
        """
        message_id = envelope.get("message_id")
        if message_id and await self.is_duplicate(message_id):
            log.debug("module=%s duplicate message_id=%s topic=%s — acking and skipping",
                      self.name, message_id, topic)
            await self.ack(topic, msg_id)
            return

        try:
            await handler(envelope, msg_id)
        except Exception:
            log.exception("module=%s handler raised on topic=%s message_id=%s",
                          self.name, topic, message_id)
            # Re-raise so the bus knows NOT to ack; PEL keeps the message
            # for retry on restart.
            raise
        else:
            await self.ack(topic, msg_id)

    # ---- heartbeat -------------------------------------------------------

    async def _emit_heartbeat_once(self) -> None:
        await self.publish(
            "health.heartbeat",
            {"pid": os.getpid(), "status": "alive"},
        )

    async def heartbeat(self) -> None:
        """Background loop: publish health.heartbeat every interval."""
        interval = settings.HEARTBEAT_INTERVAL_S
        try:
            while not self._stopping.is_set():
                try:
                    await self._emit_heartbeat_once()
                except Exception:
                    log.exception("module=%s heartbeat publish failed", self.name)
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            pass

    # ---- lifecycle -------------------------------------------------------

    async def on_start(self) -> None:
        """Called by ``run_module``. Replays each subscription's PEL,
        then starts the heartbeat task.
        """
        for pattern, group, consumer, handler in self._subscriptions:
            async def _wrapped(env: dict, mid: str, h=handler, p=pattern) -> None:
                await self.process_message(env.get("topic", p), env, mid, h)
            try:
                await self.bus.replay_pending(pattern, group, consumer, _wrapped)
            except Exception:
                log.exception("module=%s PEL replay failed for %s",
                              self.name, pattern)

        if self._heartbeat_task is None:
            self._heartbeat_task = asyncio.create_task(self.heartbeat())

    async def on_stop(self) -> None:
        """Graceful shutdown: stop heartbeat, then disconnect the bus."""
        self._stopping.set()
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass
            self._heartbeat_task = None

    @abstractmethod
    async def run(self) -> None:
        """Main loop. Implementations should `await on_start()` first."""
