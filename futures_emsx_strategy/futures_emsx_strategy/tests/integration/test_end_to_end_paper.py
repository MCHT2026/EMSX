"""End-to-end paper trading: market data -> strategy -> orders -> paper fills -> position update."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from futures_emsx_strategy.core.clock import FixedClock
from futures_emsx_strategy.core.events import MarketTick
from futures_emsx_strategy.execution.paper_adapter import PaperExecutionAdapter
from futures_emsx_strategy.market_data.bar_builder import MinuteBarBuilder
from futures_emsx_strategy.market_data.stale_data_monitor import StaleDataMonitor
from futures_emsx_strategy.market_data.tick_store import InMemoryTickStore
from futures_emsx_strategy.orders.idempotency import IdempotencyStore
from futures_emsx_strategy.orders.models import WorkingOrderBook
from futures_emsx_strategy.orders.order_manager import OrderManager
from futures_emsx_strategy.orders.throttles import RateLimiter
from futures_emsx_strategy.portfolio.exposure import ExposureCalculator
from futures_emsx_strategy.portfolio.positions import PositionBook
from futures_emsx_strategy.risk.kill_switch import KillSwitch
from futures_emsx_strategy.risk.limits import RiskLimits
from futures_emsx_strategy.risk.pre_trade import PreTradeRiskGateway
from futures_emsx_strategy.strategy.minute_strategy import MinuteFuturesStrategy


def _tick(price: float, ts: datetime) -> MarketTick:
    return MarketTick(
        instrument="ESM6 Index",
        bid=price - 0.25,
        ask=price + 0.25,
        last=price,
        volume=1,
        exchange_timestamp=ts,
        receive_timestamp=ts,
    )


def test_paper_end_to_end(instruments_cfg):
    positions = PositionBook()
    working = WorkingOrderBook()
    idemp = IdempotencyStore()
    fills_seen = []
    tick_store = InMemoryTickStore()
    clock = FixedClock(datetime(2026, 5, 20, 14, 10, tzinfo=timezone.utc))
    stale = StaleDataMonitor(max_age_seconds=3600, clock=clock)

    adapter = PaperExecutionAdapter(
        get_mark=lambda s: tick_store.last(s).last if tick_store.last(s) else None,
        tick_size_lookup=lambda s: instruments_cfg.by_symbol(s).tick_size,
    )
    adapter.on_fill(fills_seen.append)
    adapter.start()

    order_manager = OrderManager(positions, working, idemp)
    risk = PreTradeRiskGateway(
        limits=RiskLimits(
            max_order_qty=20, max_position=50, max_notional=1e9,
            stale_data_seconds=3600, require_market_session=False,
            max_orders_per_minute=1000, max_cancels_per_minute=1000,
        ),
        instruments=instruments_cfg,
        positions=positions,
        exposure=ExposureCalculator(instruments_cfg),
        stale_monitor=stale,
        kill_switch=KillSwitch(armed=True, clock=clock),
        order_rate=RateLimiter(1000, 60, clock=clock),
        get_mark=lambda s: tick_store.last(s).last if tick_store.last(s) else None,
        clock=clock,
    )

    strat = MinuteFuturesStrategy(
        strategy_id="minute_es_v1",
        instrument="ESM6 Index",
        base_qty=3,
        params={"fast_lookback": 2, "slow_lookback": 4, "max_position_contracts": 8},
    )

    bars_seen = []
    bb = MinuteBarBuilder(1)
    bb.on_bar(bars_seen.append)

    t = datetime(2026, 5, 20, 14, 0, tzinfo=timezone.utc)
    prices = [100, 101, 102, 103, 104, 105, 106, 107, 108]
    minute = 0
    for p in prices:
        for s in (5, 25, 45):
            tick = _tick(p, t + timedelta(minutes=minute, seconds=s))
            tick_store.append(tick)
            stale.on_tick(tick)
            bb.on_tick(tick)
        minute += 1
    bb.flush()

    for bar in bars_seen:
        for target in strat.on_bar(bar, positions):
            for intent in order_manager.on_target(target):
                decision = risk.validate(intent)
                if decision.decision.approved:
                    adapter.submit_order(intent)

    for fill in fills_seen:
        positions.apply_fill(fill)

    assert positions.position("ESM6 Index") > 0
