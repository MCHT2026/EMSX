"""Contract roll decision logic.

The strategy stays oblivious to which contract month it's trading — this layer
takes the strategy's TargetPosition for a logical instrument and routes it to
the correct front-month contract, emitting a synthetic flatten + open in two
contracts on roll days.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from ..config.loader import InstrumentConfig


@dataclass(frozen=True)
class RollDecision:
    should_roll: bool
    from_contract: str | None
    to_contract: str | None
    reason: str


class RollLogic:
    """Decides whether to roll, given the active contract and expiry."""

    def __init__(self, instrument: InstrumentConfig) -> None:
        self.instrument = instrument

    def evaluate(
        self,
        active_contract: str,
        next_contract: str,
        active_expiry: date,
        as_of: date,
    ) -> RollDecision:
        days_left = (active_expiry - as_of).days
        if days_left <= self.instrument.roll_days_before_expiry:
            return RollDecision(
                should_roll=True,
                from_contract=active_contract,
                to_contract=next_contract,
                reason=f"days_to_expiry={days_left}",
            )
        return RollDecision(
            should_roll=False,
            from_contract=active_contract,
            to_contract=None,
            reason=f"days_to_expiry={days_left}",
        )
