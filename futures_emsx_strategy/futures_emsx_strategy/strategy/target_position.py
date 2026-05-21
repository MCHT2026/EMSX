"""Pure function: signal + params -> TargetPosition. Kept separate to be unit-testable."""
from __future__ import annotations

from datetime import datetime

from ..core.events import TargetPosition


def build_target_position(
    strategy_id: str,
    instrument: str,
    signal: float,
    timestamp: datetime,
    base_qty: int,
    entry_threshold: float = 0.0,
    exit_threshold: float = 0.0,
    max_position: int | None = None,
) -> TargetPosition:
    """Map a raw signal to a discrete target position.

    Long if signal > entry_threshold, short if signal < -entry_threshold,
    flat inside the exit deadband. base_qty controls absolute size.
    """
    if signal > entry_threshold:
        target = base_qty
        reason = f"signal_long={signal:.6f}"
    elif signal < -entry_threshold:
        target = -base_qty
        reason = f"signal_short={signal:.6f}"
    elif abs(signal) <= exit_threshold:
        target = 0
        reason = f"signal_flat={signal:.6f}"
    else:
        target = 0
        reason = f"signal_neutral={signal:.6f}"
    if max_position is not None:
        target = max(-max_position, min(max_position, target))
    return TargetPosition(
        strategy_id=strategy_id,
        instrument=instrument,
        target_qty=target,
        timestamp=timestamp,
        reason=reason,
        metadata={"signal": signal},
    )
