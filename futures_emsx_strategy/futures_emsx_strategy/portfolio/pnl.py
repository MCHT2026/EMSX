"""Realized + unrealized PnL in currency terms, using each contract's point value."""
from __future__ import annotations

from dataclasses import dataclass
from threading import Lock

from ..config.loader import InstrumentsConfig
from ..core.events import FillUpdate


@dataclass
class PnLSnapshot:
    realized: float
    unrealized: float
    total: float


class PnLCalculator:
    def __init__(self, instruments: InstrumentsConfig) -> None:
        self._instruments = instruments
        self._realized: dict[str, float] = {}
        self._positions: dict[str, tuple[int, float]] = {}
        self._lock = Lock()

    def apply_fill(self, fill: FillUpdate) -> None:
        with self._lock:
            ins = self._instruments.by_symbol(fill.instrument)
            qty, avg = self._positions.get(fill.instrument, (0, 0.0))
            signed_qty = fill.fill_qty * fill.side.sign
            realized = self._realized.get(fill.instrument, 0.0)
            if qty == 0 or (qty > 0) == (signed_qty > 0):
                new_qty = qty + signed_qty
                if new_qty != 0:
                    avg = (avg * qty + fill.fill_price * signed_qty) / new_qty
                else:
                    avg = 0.0
                qty = new_qty
            else:
                close_qty = min(abs(qty), abs(signed_qty))
                pnl_per_unit = (fill.fill_price - avg) * (1 if qty > 0 else -1)
                realized += pnl_per_unit * close_qty * ins.point_value
                qty += signed_qty
                if qty == 0:
                    avg = 0.0
                elif (qty > 0) != ((qty - signed_qty) > 0):
                    avg = fill.fill_price
            self._realized[fill.instrument] = realized
            self._positions[fill.instrument] = (qty, avg)

    def unrealized(self, marks: dict[str, float]) -> float:
        total = 0.0
        with self._lock:
            for sym, (qty, avg) in self._positions.items():
                mark = marks.get(sym)
                if mark is None or qty == 0:
                    continue
                ins = self._instruments.by_symbol(sym)
                total += (mark - avg) * qty * ins.point_value
        return total

    def realized(self) -> float:
        with self._lock:
            return sum(self._realized.values())

    def snapshot(self, marks: dict[str, float]) -> PnLSnapshot:
        r = self.realized()
        u = self.unrealized(marks)
        return PnLSnapshot(realized=r, unrealized=u, total=r + u)
