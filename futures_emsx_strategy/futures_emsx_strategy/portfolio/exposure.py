"""Notional and gross/net exposure computations for risk gating."""
from __future__ import annotations

from dataclasses import dataclass

from ..config.loader import InstrumentsConfig
from .positions import PositionBook


@dataclass
class Exposure:
    instrument: str
    qty: int
    notional: float


class ExposureCalculator:
    def __init__(self, instruments: InstrumentsConfig) -> None:
        self._instruments = instruments

    def notional(self, instrument: str, qty: int, price: float) -> float:
        ins = self._instruments.by_symbol(instrument)
        return abs(qty) * price * ins.point_value

    def gross_notional(self, positions: PositionBook, marks: dict[str, float]) -> float:
        total = 0.0
        for sym, (qty, _avg) in positions.snapshot().items():
            mark = marks.get(sym)
            if mark is None:
                continue
            total += self.notional(sym, qty, mark)
        return total

    def per_instrument(
        self, positions: PositionBook, marks: dict[str, float]
    ) -> list[Exposure]:
        rows = []
        for sym, (qty, _avg) in positions.snapshot().items():
            mark = marks.get(sym, 0.0)
            rows.append(Exposure(sym, qty, self.notional(sym, qty, mark)))
        return rows
