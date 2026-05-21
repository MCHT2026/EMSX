"""Per-instrument indicator state held by a strategy."""
from __future__ import annotations

from dataclasses import dataclass, field

from .indicators import EMA


@dataclass
class SignalState:
    fast_lookback: int
    slow_lookback: int
    fast: EMA = field(init=False)
    slow: EMA = field(init=False)
    last_signal: float | None = None

    def __post_init__(self) -> None:
        self.fast = EMA(self.fast_lookback)
        self.slow = EMA(self.slow_lookback)

    def update(self, close: float) -> float | None:
        f = self.fast.update(close)
        s = self.slow.update(close)
        if f is None or s is None:
            return None
        sig = f - s
        self.last_signal = sig
        return sig

    @property
    def ready(self) -> bool:
        return self.fast.ready and self.slow.ready
