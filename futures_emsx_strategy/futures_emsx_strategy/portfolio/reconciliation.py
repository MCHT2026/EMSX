"""Reconciles internal state against EMSX and broker/clearing."""
from __future__ import annotations

from dataclasses import dataclass

from ..core.logging import get_logger
from .positions import PositionBook

log = get_logger(__name__)


@dataclass
class PositionBreak:
    instrument: str
    internal_qty: int
    external_qty: int

    @property
    def delta(self) -> int:
        return self.external_qty - self.internal_qty


class Reconciler:
    def __init__(self, internal: PositionBook) -> None:
        self.internal = internal

    def compare(self, external_positions: dict[str, int]) -> list[PositionBreak]:
        breaks: list[PositionBreak] = []
        symbols = set(external_positions.keys()) | {
            s for s, _ in self.internal.snapshot().items()
        }
        for sym in symbols:
            internal_qty = self.internal.position(sym)
            external_qty = external_positions.get(sym, 0)
            if internal_qty != external_qty:
                breaks.append(
                    PositionBreak(
                        instrument=sym,
                        internal_qty=internal_qty,
                        external_qty=external_qty,
                    )
                )
        if breaks:
            log.warning("position_breaks_detected", count=len(breaks))
        return breaks

    def force_resync(self, external_positions: dict[str, int]) -> None:
        """Trust external source. Use only with explicit operator approval."""
        for sym, qty in external_positions.items():
            self.internal.set(sym, qty)
        log.warning("position_force_resynced", symbols=list(external_positions.keys()))
