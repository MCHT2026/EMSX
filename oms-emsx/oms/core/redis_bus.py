"""Redis Streams implementation of EventBus.

Spec mapping (oms_emsx_spec.md):

- Each topic maps to a Redis stream of the same name (``orders.new``,
  ``fills.partial``, ...).
- Consumer group name = module owner_id; consumer name = ``{owner}:{host}``.
- Wildcard subscriptions are resolved from a topic registry — a Redis SET
  named ``oms:topics`` to which every publish writes. Subscribers refresh
  their resolved list periodically.
- Envelope is serialised to JSON and stored in a single stream field
  ``data``.
- ``XREADGROUP`` runs with ``COUNT=10`` and ``BLOCK=100`` per the spec.
- Outbound publishes during a transient Redis outage land in an
  ``asyncio.Queue(maxsize=LOCAL_BUFFER_MAXSIZE)`` and drain in order on
  reconnect.
- Reconnect: exponential backoff starting at 100ms, capped at 10s,
  indefinite retries.

Note on aioredis: ``aioredis`` was merged into ``redis-py`` in v4.2; this
module uses ``redis.asyncio`` which is the supported successor with the
same async API.
"""
from __future__ import annotations

import asyncio
import fnmatch
import json
import logging
from typing import Any, Awaitable, Callable

import redis.asyncio as aioredis
from redis.asyncio.sentinel import Sentinel

from config import settings
from core.event_bus import EventBus, MessageHandler

log = logging.getLogger(__name__)

TOPIC_REGISTRY_KEY = "oms:topics"


class RedisBus(EventBus):
    """Redis-Streams event bus with optional Sentinel HA + local buffer."""

    def __init__(
        self,
        redis_client: aioredis.Redis | None = None,
        sentinel_hosts: list[tuple[str, int]] | None = None,
        sentinel_master: str = "mymaster",
        password: str | None = None,
        buffer_maxsize: int | None = None,
    ) -> None:
        self.redis: aioredis.Redis | None = redis_client
        self._sentinel_hosts = sentinel_hosts or settings.REDIS_SENTINEL_HOSTS
        self._sentinel_master = sentinel_master or settings.REDIS_SENTINEL_MASTER
        self._password = password if password is not None else settings.REDIS_PASSWORD
        self._buffer: asyncio.Queue[tuple[str, dict]] = asyncio.Queue(
            maxsize=buffer_maxsize or settings.LOCAL_BUFFER_MAXSIZE
        )
        self._connected = redis_client is not None
        self._sentinel: Sentinel | None = None
        self._subscriber_tasks: list[asyncio.Task] = []

    # ---- lifecycle --------------------------------------------------------

    async def connect(self) -> None:
        if self.redis is not None:
            self._connected = True
            return
        if settings.REDIS_DIRECT_URL:
            self.redis = aioredis.from_url(
                settings.REDIS_DIRECT_URL,
                decode_responses=False,
            )
        else:
            self._sentinel = Sentinel(
                self._sentinel_hosts,
                password=self._password,
            )
            self.redis = self._sentinel.master_for(
                self._sentinel_master,
                password=self._password,
                decode_responses=False,
            )
        # Sanity check the connection.
        await self.redis.ping()
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False
        for task in self._subscriber_tasks:
            task.cancel()
        for task in self._subscriber_tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._subscriber_tasks.clear()
        if self.redis is not None:
            try:
                await self.redis.aclose()
            except Exception:
                pass

    # ---- publish ----------------------------------------------------------

    async def publish(self, topic: str, payload: dict) -> str:
        """Publish *payload* (already-stamped envelope) on *topic*.

        If the bus is currently disconnected, the message lands in the
        local buffer; an empty string is returned and ``drain_buffer()``
        will publish it on reconnect. If the buffer is full, drops the
        oldest message and logs — at-most-once during outage is preferable
        to blocking the strategy loop.
        """
        if not self._connected or self.redis is None:
            try:
                self._buffer.put_nowait((topic, payload))
            except asyncio.QueueFull:
                # Drop oldest, enqueue new.
                try:
                    self._buffer.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                self._buffer.put_nowait((topic, payload))
                log.warning("bus buffer full — dropped oldest message")
            return ""

        # Register topic so wildcard subscribers can discover it.
        try:
            await self.redis.sadd(TOPIC_REGISTRY_KEY, topic)
        except Exception:
            log.exception("bus failed to register topic %s", topic)

        data = json.dumps(payload, default=str).encode("utf-8")
        mid = await self.redis.xadd(topic, {"data": data})
        return mid.decode() if isinstance(mid, bytes) else str(mid)

    async def drain_buffer(self) -> None:
        """Drain the local outbound buffer in FIFO order onto Redis."""
        while not self._buffer.empty():
            topic, payload = await self._buffer.get()
            try:
                await self.publish(topic, payload)
            except Exception:
                # Put it back at the head and bail; caller will retry.
                # (Queue has no put-front, so re-enqueue at tail and stop.)
                try:
                    self._buffer.put_nowait((topic, payload))
                except asyncio.QueueFull:
                    pass
                raise

    # ---- subscribe --------------------------------------------------------

    async def ensure_group(self, topic: str, group: str) -> None:
        """Idempotently create the consumer group on *topic*."""
        try:
            await self.redis.xgroup_create(topic, group, id="0", mkstream=True)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def resolve_topics(self, pattern: str) -> list[str]:
        """Return all topics in the registry matching *pattern*."""
        members = await self.redis.smembers(TOPIC_REGISTRY_KEY)
        names = [m.decode() if isinstance(m, bytes) else m for m in members]
        if pattern in ("*", ""):
            return sorted(names)
        return sorted(n for n in names if fnmatch.fnmatchcase(n, pattern))

    async def consume_once(
        self,
        topic: str,
        group: str,
        consumer: str,
        handler: MessageHandler,
    ) -> int:
        """One XREADGROUP cycle. Returns the number of messages dispatched."""
        if self.redis is None:
            return 0
        resp = await self.redis.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={topic: ">"},
            count=settings.XREAD_COUNT,
            block=settings.XREAD_BLOCK_MS,
        )
        delivered = 0
        for stream_name, entries in resp or []:
            for raw_id, fields in entries:
                mid = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
                payload = fields.get(b"data") or fields.get("data")
                envelope = json.loads(payload) if payload else {}
                try:
                    await handler(envelope, mid)
                except Exception:
                    log.exception("bus handler error topic=%s mid=%s", topic, mid)
                    # Do NOT ack — let PEL pick it up on replay.
                    continue
                delivered += 1
        return delivered

    async def subscribe(
        self,
        topic_pattern: str,
        group: str,
        consumer: str,
        handler: MessageHandler,
    ) -> None:
        """Spawn a background task that consumes messages forever.

        For wildcard patterns we resolve at subscribe time and refresh
        every 30 seconds — new topics matching the pattern get picked up
        on the next refresh.
        """
        topics = await self.resolve_topics(topic_pattern)
        if not topics and topic_pattern.startswith("*") is False and "*" not in topic_pattern:
            # Concrete topic that doesn't exist yet — create the stream
            # so XREADGROUP doesn't error.
            topics = [topic_pattern]

        for t in topics:
            await self.ensure_group(t, group)

        async def _runner() -> None:
            resolved = list(topics)
            last_refresh = 0.0
            while self._connected:
                # Refresh wildcard resolution every 30s.
                if "*" in topic_pattern:
                    now = asyncio.get_event_loop().time()
                    if now - last_refresh > 30.0:
                        new = await self.resolve_topics(topic_pattern)
                        for t in new:
                            if t not in resolved:
                                await self.ensure_group(t, group)
                                resolved.append(t)
                        last_refresh = now

                for t in resolved:
                    try:
                        await self.consume_once(t, group, consumer, handler)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        log.exception("bus consume failed on %s", t)
                        await asyncio.sleep(0.1)
                # Small yield so we don't spin if all streams empty.
                await asyncio.sleep(0)

        task = asyncio.create_task(_runner(), name=f"sub:{group}:{topic_pattern}")
        self._subscriber_tasks.append(task)

    # ---- ack + pending ----------------------------------------------------

    async def ack(self, topic: str, group: str, message_id: str) -> None:
        if self.redis is None:
            return
        await self.redis.xack(topic, group, message_id)

    async def get_pending(self, topic: str, group: str) -> list:
        if self.redis is None:
            return []
        # Use the detailed form: XPENDING <key> <group> - + 1000 (range query)
        try:
            res = await self.redis.xpending_range(topic, group, min="-", max="+", count=1000)
        except aioredis.ResponseError:
            return []
        # Each entry is a dict {message_id, consumer, time_since_delivered,
        # times_delivered} in redis-py.
        return list(res)

    async def replay_pending(
        self,
        topic: str,
        group: str,
        consumer: str,
        handler: MessageHandler,
    ) -> None:
        if self.redis is None:
            return
        pending = await self.get_pending(topic, group)
        if not pending:
            return
        ids = [e["message_id"] for e in pending]
        # XCLAIM the messages to *this* consumer (min-idle-time=0) so we own
        # them, then read the actual envelopes via XRANGE for each.
        claimed = await self.redis.xclaim(
            topic, group, consumer, min_idle_time=0, message_ids=ids
        )
        for raw_id, fields in claimed:
            mid = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
            payload = fields.get(b"data") or fields.get("data")
            envelope = json.loads(payload) if payload else {}
            try:
                await handler(envelope, mid)
            except Exception:
                log.exception("bus replay handler failed topic=%s mid=%s", topic, mid)

    # ---- idempotency (SET NX with TTL) -----------------------------------

    async def set_nx(self, key: str, ttl_s: int) -> bool:
        if self.redis is None:
            return False
        ok = await self.redis.set(key, "1", nx=True, ex=ttl_s)
        return bool(ok)

    # ---- Sentinel pub/sub -------------------------------------------------

    async def watch_sentinel_switches(
        self, handler: Callable[[dict], Awaitable[None]]
    ) -> None:
        """Subscribe to ``+switch-master`` to detect failovers.

        Used by the watchdog. Each event calls *handler* with the parsed
        message dict; cancellation stops the subscription.
        """
        if self._sentinel is None:
            log.debug("RedisBus has no Sentinel — switch-master watch disabled")
            return
        sentinel_master = self._sentinel.discover_master(self._sentinel_master)
        sentinel_conn = self._sentinel.sentinels[0]
        pubsub = sentinel_conn.pubsub()
        await pubsub.subscribe("+switch-master")
        try:
            async for msg in pubsub.listen():
                if msg.get("type") == "message":
                    await handler(msg)
        except asyncio.CancelledError:
            await pubsub.unsubscribe("+switch-master")
            raise
