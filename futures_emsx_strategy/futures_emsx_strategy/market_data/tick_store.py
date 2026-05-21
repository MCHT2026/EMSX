"""In-memory ring buffer of recent ticks per instrument. Used for diagnostics and stale checks."""
from __future__ import annotations

from collections import deque
from threading import Lock

from ..core.events import MarketTick


class InMemoryTickStore:
    def __init__(self, max_per_instrument: int = 1024) -> None:
        self._buffers: dict[str, deque[MarketTick]] = {}
        self._max = max_per_instrument
        self._lock = Lock()

    def append(self, tick: MarketTick) -> None:
        with self._lock:
            buf = self._buffers.get(tick.instrument)
            if buf is None:
                buf = deque(maxlen=self._max)
                self._buffers[tick.instrument] = buf
            buf.append(tick)

    def last(self, instrument: str) -> MarketTick | None:
        with self._lock:
            buf = self._buffers.get(instrument)
            return buf[-1] if buf else None

    def recent(self, instrument: str, n: int = 100) -> list[MarketTick]:
        with self._lock:
            buf = self._buffers.get(instrument)
            if not buf:
                return []
            return list(buf)[-n:]

    def instruments(self) -> list[str]:
        with self._lock:
            return list(self._buffers.keys())
