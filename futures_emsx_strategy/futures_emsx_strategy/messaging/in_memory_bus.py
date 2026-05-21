"""Synchronous in-memory bus. Use for single-process deployments and tests."""
from __future__ import annotations

from collections import defaultdict
from threading import Lock
from typing import Any

from ..core.logging import get_logger
from .bus import EventBus, Handler

log = get_logger(__name__)


class InMemoryBus(EventBus):
    def __init__(self) -> None:
        self._subs: dict[str, list[Handler]] = defaultdict(list)
        self._lock = Lock()

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def publish(self, topic: str, payload: Any) -> None:
        with self._lock:
            handlers = list(self._subs.get(topic, []))
        for h in handlers:
            try:
                h(topic, payload)
            except Exception:  # noqa: BLE001
                log.exception("bus_handler_failed", topic=topic)

    def subscribe(self, topic: str, handler: Handler) -> None:
        with self._lock:
            self._subs[topic].append(handler)
