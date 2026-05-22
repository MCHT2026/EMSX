"""Abstract event-bus interface.

All implementations (`core.redis_bus.RedisBus`, the in-memory test bus, etc.)
must satisfy this contract. The interface is intentionally narrow: publish,
subscribe via consumer groups, ack, list and replay pending entries,
connect/disconnect.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable

# A message handler is an async callable that receives the full envelope
# (owner_id / message_id / timestamp / topic / data) and the Redis stream
# message id (so the caller can XACK after successful handling).
MessageHandler = Callable[[dict[str, Any], str], Awaitable[None]]


class EventBus(ABC):
    """Abstract event bus. All transports implement this contract."""

    @abstractmethod
    async def publish(self, topic: str, payload: dict) -> str:
        """Publish *payload* on *topic*. Returns the stream message id."""

    @abstractmethod
    async def subscribe(
        self,
        topic_pattern: str,
        group: str,
        consumer: str,
        handler: MessageHandler,
    ) -> None:
        """Subscribe *consumer* (in consumer-*group*) to *topic_pattern*.

        Glob patterns (e.g. ``fills.*``) match all known topics at the time
        the subscription is registered; new topics matching the pattern are
        picked up on the bus's topic-registry refresh interval.
        """

    @abstractmethod
    async def ack(self, topic: str, group: str, message_id: str) -> None:
        """``XACK`` *message_id* on *topic* for *group*."""

    @abstractmethod
    async def get_pending(self, topic: str, group: str) -> list:
        """Return the pending-entries list (PEL) for *group* on *topic*."""

    @abstractmethod
    async def replay_pending(
        self,
        topic: str,
        group: str,
        consumer: str,
        handler: MessageHandler,
    ) -> None:
        """Claim and redeliver all of *consumer*'s pending messages."""

    @abstractmethod
    async def connect(self) -> None:
        """Open the connection (or pool) to the underlying transport."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close the connection cleanly."""
