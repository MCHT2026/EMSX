"""Stateful streaming indicators. Update incrementally with each bar close."""
from __future__ import annotations

import math
from collections import deque


class SMA:
    def __init__(self, period: int) -> None:
        if period <= 0:
            raise ValueError("period must be positive")
        self.period = period
        self._buf: deque[float] = deque(maxlen=period)
        self._sum = 0.0

    def update(self, x: float) -> float | None:
        if len(self._buf) == self.period:
            self._sum -= self._buf[0]
        self._buf.append(x)
        self._sum += x
        if len(self._buf) < self.period:
            return None
        return self._sum / self.period

    @property
    def ready(self) -> bool:
        return len(self._buf) == self.period


class EMA:
    def __init__(self, period: int) -> None:
        if period <= 0:
            raise ValueError("period must be positive")
        self.period = period
        self.alpha = 2.0 / (period + 1)
        self.value: float | None = None
        self._count = 0

    def update(self, x: float) -> float | None:
        self._count += 1
        if self.value is None:
            self.value = x
        else:
            self.value = self.alpha * x + (1.0 - self.alpha) * self.value
        return self.value if self._count >= self.period else None

    @property
    def ready(self) -> bool:
        return self._count >= self.period


class RollingZ:
    """Streaming z-score. None until `period` samples are accumulated."""

    def __init__(self, period: int) -> None:
        if period < 2:
            raise ValueError("period must be >= 2")
        self.period = period
        self._buf: deque[float] = deque(maxlen=period)

    def update(self, x: float) -> float | None:
        self._buf.append(x)
        if len(self._buf) < self.period:
            return None
        mean = sum(self._buf) / self.period
        var = sum((v - mean) ** 2 for v in self._buf) / (self.period - 1)
        sd = math.sqrt(var)
        if sd == 0:
            return 0.0
        return (x - mean) / sd

    @property
    def ready(self) -> bool:
        return len(self._buf) == self.period
