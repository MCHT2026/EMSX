"""Position book: net position and weighted-average cost per instrument."""
from __future__ import annotations

from dataclasses import dataclass
from threading import Lock

from ..core.enums import Side
from ..core.events import FillUpdate


@dataclass
class _PositionRow:
    qty: int = 0
    avg_cost: float = 0.0


class PositionBook:
    def __init__(self) -> None:
        self._rows: dict[str, _PositionRow] = {}
        self._lock = Lock()

    def position(self, instrument: str) -> int:
        with self._lock:
            return self._rows.get(instrument, _PositionRow()).qty

    def avg_cost(self, instrument: str) -> float | None:
        with self._lock:
            row = self._rows.get(instrument)
            if row is None or row.qty == 0:
                return None
            return row.avg_cost

    def apply_fill(self, fill: FillUpdate) -> None:
        """Update position and weighted-avg-cost. Realized PnL is computed in PnLCalculator."""
        with self._lock:
            row = self._rows.setdefault(fill.instrument, _PositionRow())
            signed_qty = fill.fill_qty * fill.side.sign
            if row.qty == 0 or (row.qty > 0) == (signed_qty > 0):
                new_qty = row.qty + signed_qty
                if new_qty == 0:
                    row.avg_cost = 0.0
                else:
                    row.avg_cost = (
                        row.avg_cost * row.qty + fill.fill_price * signed_qty
                    ) / new_qty
                row.qty = new_qty
            else:
                row.qty += signed_qty
                if row.qty == 0:
                    row.avg_cost = 0.0
                elif (row.qty > 0) != (signed_qty > 0):
                    row.avg_cost = fill.fill_price

    def snapshot(self) -> dict[str, tuple[int, float]]:
        with self._lock:
            return {k: (v.qty, v.avg_cost) for k, v in self._rows.items()}

    def set(self, instrument: str, qty: int, avg_cost: float = 0.0) -> None:
        """Hard-set a position. Used by reconciliation when start-of-day positions are loaded."""
        with self._lock:
            self._rows[instrument] = _PositionRow(qty=qty, avg_cost=avg_cost)

    def is_flat(self, instrument: str) -> bool:
        return self.position(instrument) == 0

    @staticmethod
    def side_for_delta(delta: int) -> Side:
        return Side.from_delta(delta)
