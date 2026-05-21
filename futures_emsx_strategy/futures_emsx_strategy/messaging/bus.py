"""Bus interface. Topics are strings; payloads are arbitrary dataclasses or dicts."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable

Handler = Callable[[str, Any], None]


class EventBus(ABC):
    @abstractmethod
    def publish(self, topic: str, payload: Any) -> None: ...

    @abstractmethod
    def subscribe(self, topic: str, handler: Handler) -> None: ...

    @abstractmethod
    def start(self) -> None: ...

    @abstractmethod
    def stop(self) -> None: ...
