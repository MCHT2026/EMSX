"""Backtest engine: feeds bars through Strategy + OrderManager + Portfolio, simulating fills.

Reuses the live components (Strategy, OrderManager, PositionBook, FillLedger, PnLCalculator)
so backtest and live share exactly the same trading logic.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

from ..config.loader import InstrumentsConfig
from ..core.clock import SimulatedClock
from ..core.enums import Side
from ..core.events import BarClosed, FillUpdate, OrderIntent, TargetPosition
from ..core.logging import get_logger
from ..orders.idempotency import IdempotencyStore
from ..orders.models import WorkingOrderBook
from ..orders.order_manager import OrderManager
from ..portfolio.fills import FillLedger
from ..portfolio.pnl import PnLCalculator, PnLSnapshot
from ..portfolio.positions import PositionBook
from ..strategy.base import Strategy
from .commission import CommissionModel, FlatCommissionModel
from .slippage import FixedTickSlippage, SlippageModel

log = get_logger(__name__)


@dataclass
class BacktestResult:
    bars_processed: int
    targets_emitted: int
    intents_emitted: int
    fills_count: int
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    total_commission: float
    end_positions: dict[str, int] = field(default_factory=dict)
    timeline: list[tuple[datetime, PnLSnapshot]] = field(default_factory=list)


class BacktestEngine:
    def __init__(
        self,
        strategy: Strategy,
        instruments: InstrumentsConfig,
        slippage: SlippageModel | None = None,
        commission: CommissionModel | None = None,
    ) -> None:
        self.strategy = strategy
        self.instruments = instruments
        self.positions = PositionBook()
        self.working = WorkingOrderBook()
        self.idempotency = IdempotencyStore()
        self.fills = FillLedger()
        self.pnl = PnLCalculator(instruments)
        self.order_manager = OrderManager(self.positions, self.working, self.idempotency)
        self.commission = commission or FlatCommissionModel()
        self.slippage = slippage or FixedTickSlippage(
            ticks=1.0,
            tick_size_lookup=lambda s: instruments.by_symbol(s).tick_size,
        )
        self.clock = SimulatedClock(start=datetime.fromtimestamp(0, tz=timezone.utc))

    def run(self, bars: Iterable[BarClosed]) -> BacktestResult:
        result = BacktestResult(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0)
        marks: dict[str, float] = {}
        total_commission = 0.0
        last_close: dict[str, float] = {}
        for bar in bars:
            self.clock.advance_to(bar.end_time)
            marks[bar.instrument] = bar.close
            last_close[bar.instrument] = bar.close
            result.bars_processed += 1

            targets: list[TargetPosition] = self.strategy.on_bar(bar, self.positions)
            result.targets_emitted += len(targets)
            for target in targets:
                intents: list[OrderIntent] = self.order_manager.on_target(target)
                result.intents_emitted += len(intents)
                for intent in intents:
                    fill_price = self.slippage.adjust(bar.close, intent.side, intent.instrument)
                    fill = FillUpdate(
                        order_id=f"BT-{result.fills_count + 1}",
                        route_id=None,
                        instrument=intent.instrument,
                        side=intent.side,
                        fill_qty=intent.qty,
                        fill_price=fill_price,
                        timestamp=bar.end_time,
                    )
                    self.positions.apply_fill(fill)
                    self.pnl.apply_fill(fill)
                    self.fills.record(fill)
                    total_commission += self.commission.per_fill(
                        intent.qty, fill_price, intent.instrument
                    )
                    result.fills_count += 1

            snap = self.pnl.snapshot(marks)
            result.timeline.append((bar.end_time, snap))

        snap = self.pnl.snapshot(last_close)
        result.realized_pnl = snap.realized
        result.unrealized_pnl = snap.unrealized
        result.total_pnl = snap.total
        result.total_commission = total_commission
        result.end_positions = {k: v[0] for k, v in self.positions.snapshot().items()}
        log.info(
            "backtest_complete",
            bars=result.bars_processed,
            fills=result.fills_count,
            realized=result.realized_pnl,
            unrealized=result.unrealized_pnl,
            commission=result.total_commission,
        )
        return result

    def position_for(self, instrument: str) -> int:
        return self.positions.position(instrument)

    def side_for(self, delta: int) -> Side:
        return Side.from_delta(delta)
