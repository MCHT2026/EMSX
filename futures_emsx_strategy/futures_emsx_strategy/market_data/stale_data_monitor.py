"""Tracks per-instrument last-tick time so pre-trade risk can refuse to trade on stale data."""
from __future__ import annotations

from datetime import datetime, timezone
from threading import Lock

from ..core.clock import Clock, SystemClock
from ..core.events import MarketTick


class StaleDataMonitor:
    def __init__(self, max_age_seconds: int, clock: Clock | None = None) -> None:
        self.max_age_seconds = max_age_seconds
        self._last_seen: dict[str, datetime] = {}
        self._lock = Lock()
        self._clock = clock or SystemClock()

    def on_tick(self, tick: MarketTick) -> None:
        with self._lock:
            self._last_seen[tick.instrument] = tick.receive_timestamp

    def last_seen(self, instrument: str) -> datetime | None:
        with self._lock:
            return self._last_seen.get(instrument)

    def is_stale(self, instrument: str) -> bool:
        now = self._clock.now()
        last = self.last_seen(instrument)
        if last is None:
            return True
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        age = (now - last).total_seconds()
        return age > self.max_age_seconds

    def age_seconds(self, instrument: str) -> float | None:
        last = self.last_seen(instrument)
        if last is None:
            return None
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        return (self._clock.now() - last).total_seconds()
