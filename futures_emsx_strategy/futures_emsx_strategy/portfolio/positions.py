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
        """Update position and weighted-avg-cost. Realized PnL is in PnLCalculator.

        Rules:
          - Adding to an existing position (or opening from flat): weighted avg.
          - Partial close (same sign before and after): avg unchanged.
          - Full close (lands on zero): avg reset to 0.
          - Flip (sign changes through zero): new avg is the fill price.
        """
        with self._lock:
            row = self._rows.setdefault(fill.instrument, _PositionRow())
            signed_qty = fill.fill_qty * fill.side.sign
            prior_qty = row.qty
            new_qty = prior_qty + signed_qty

            if prior_qty == 0 or (prior_qty > 0) == (signed_qty > 0):
                # Opening from flat, or stacking on the same side.
                if new_qty == 0:
                    row.avg_cost = 0.0
                else:
                    row.avg_cost = (
                        row.avg_cost * prior_qty + fill.fill_price * signed_qty
                    ) / new_qty
            else:
                # Opposite side: close, full close, or flip.
                if new_qty == 0:
                    row.avg_cost = 0.0
                elif (prior_qty > 0) != (new_qty > 0):
                    # Crossed through zero -> flipped sides. New basis = fill price.
                    row.avg_cost = fill.fill_price
                # else: partial close, leave avg_cost as it was.
            row.qty = new_qty

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
