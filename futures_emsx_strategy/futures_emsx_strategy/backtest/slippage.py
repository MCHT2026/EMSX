"""Slippage models for backtests."""
from __future__ import annotations

from abc import ABC, abstractmethod

from ..core.enums import Side


class SlippageModel(ABC):
    @abstractmethod
    def adjust(self, mid: float, side: Side, instrument: str) -> float: ...


class FixedTickSlippage(SlippageModel):
    def __init__(self, ticks: float, tick_size_lookup) -> None:
        self.ticks = ticks
        self.tick_size_lookup = tick_size_lookup

    def adjust(self, mid: float, side: Side, instrument: str) -> float:
        ts = self.tick_size_lookup(instrument)
        return mid + self.ticks * ts * side.sign
