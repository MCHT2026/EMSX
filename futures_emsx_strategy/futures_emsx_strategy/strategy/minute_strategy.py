"""Reference minute-bar futures momentum strategy.

Inputs: BarClosed events.
Outputs: TargetPosition events.

Long when fast EMA > slow EMA, short when fast < slow, flat in deadband.
The strategy is intentionally simple — the value of this codebase is the
infrastructure around it, not the signal.
"""
from __future__ import annotations

from typing import Any

from ..core.events import BarClosed, TargetPosition
from ..core.logging import get_logger
from .base import PortfolioView, Strategy
from .signal_state import SignalState
from .target_position import build_target_position

log = get_logger(__name__)


class MinuteFuturesStrategy(Strategy):
    def __init__(
        self,
        strategy_id: str,
        instrument: str,
        base_qty: int,
        params: dict[str, Any] | None = None,
    ) -> None:
        self.strategy_id = strategy_id
        self.instrument = instrument
        self.base_qty = base_qty
        p = params or {}
        self.fast_lookback = int(p.get("fast_lookback", 5))
        self.slow_lookback = int(p.get("slow_lookback", 20))
        self.entry_threshold = float(p.get("entry_threshold", 0.0))
        self.exit_threshold = float(p.get("exit_threshold", 0.0))
        self.max_position = int(p.get("max_position_contracts", base_qty))
        self._state = SignalState(self.fast_lookback, self.slow_lookback)

    def on_bar(self, bar: BarClosed, portfolio: PortfolioView) -> list[TargetPosition]:
        if bar.instrument != self.instrument:
            return []
        signal = self._state.update(bar.close)
        if signal is None:
            return []
        target = build_target_position(
            strategy_id=self.strategy_id,
            instrument=self.instrument,
            signal=signal,
            timestamp=bar.end_time,
            base_qty=self.base_qty,
            entry_threshold=self.entry_threshold,
            exit_threshold=self.exit_threshold,
            max_position=self.max_position,
        )
        log.info(
            "target_emitted",
            strategy=self.strategy_id,
            instrument=self.instrument,
            signal=signal,
            target=target.target_qty,
            bar_end=bar.end_time.isoformat(),
            current_position=portfolio.position(self.instrument),
        )
        return [target]
