"""Pub/sub bus. In-memory for single-process; Kafka/Redis for multi-service."""
from .bus import EventBus
from .in_memory_bus import InMemoryBus
from .kafka_bus import KafkaBus
from .redis_bus import RedisStreamBus

__all__ = ["EventBus", "InMemoryBus", "KafkaBus", "RedisStreamBus"]


def make_bus(kind: str, url: str | None = None) -> EventBus:
    if kind == "memory":
        return InMemoryBus()
    if kind == "kafka":
        if url is None:
            raise ValueError("kafka bus requires bus_url")
        return KafkaBus(url)
    if kind == "redis":
        if url is None:
            raise ValueError("redis bus requires bus_url")
        return RedisStreamBus(url)
    raise ValueError(f"Unknown bus kind: {kind}")
