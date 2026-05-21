"""Abstracted clock so backtests and live trading share code paths."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone


class Clock(ABC):
    @abstractmethod
    def now(self) -> datetime: ...


class SystemClock(Clock):
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class FixedClock(Clock):
    def __init__(self, fixed: datetime) -> None:
        self._t = fixed

    def now(self) -> datetime:
        return self._t

    def set(self, t: datetime) -> None:
        self._t = t


class SimulatedClock(Clock):
    """Monotonic clock driven by replay events. Used in backtests."""

    def __init__(self, start: datetime) -> None:
        self._t = start

    def now(self) -> datetime:
        return self._t

    def advance_to(self, t: datetime) -> None:
        if t < self._t:
            raise ValueError(f"cannot rewind simulated clock from {self._t} to {t}")
        self._t = t
