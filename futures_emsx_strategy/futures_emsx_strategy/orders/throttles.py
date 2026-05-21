"""Sliding-window rate limiter (orders/minute, cancels/minute)."""
from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta
from threading import Lock

from ..core.clock import Clock, SystemClock


class RateLimiter:
    def __init__(self, max_events: int, window_seconds: int, clock: Clock | None = None) -> None:
        self.max_events = max_events
        self.window = timedelta(seconds=window_seconds)
        self._events: deque[datetime] = deque()
        self._lock = Lock()
        self._clock = clock or SystemClock()

    def try_acquire(self) -> bool:
        now = self._clock.now()
        with self._lock:
            self._evict(now)
            if len(self._events) >= self.max_events:
                return False
            self._events.append(now)
            return True

    def current(self) -> int:
        now = self._clock.now()
        with self._lock:
            self._evict(now)
            return len(self._events)

    def _evict(self, now: datetime) -> None:
        threshold = now - self.window
        while self._events and self._events[0] < threshold:
            self._events.popleft()
