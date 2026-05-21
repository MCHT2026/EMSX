"""Regression: rate-limit token is not consumed when an earlier check rejects."""
from __future__ import annotations

from datetime import datetime, timezone

from futures_emsx_strategy.config.loader import InstrumentConfig, InstrumentsConfig
from futures_emsx_strategy.core.clock import FixedClock
from futures_emsx_strategy.core.enums import OrderType, Side, TimeInForce
from futures_emsx_strategy.core.events import OrderIntent
from futures_emsx_strategy.market_data.stale_data_monitor import StaleDataMonitor
from futures_emsx_strategy.orders.throttles import RateLimiter
from futures_emsx_strategy.portfolio.exposure import ExposureCalculator
from futures_emsx_strategy.portfolio.positions import PositionBook
from futures_emsx_strategy.risk.kill_switch import KillSwitch
from futures_emsx_strategy.risk.limits import RiskLimits
from futures_emsx_strategy.risk.pre_trade import PreTradeRiskGateway


def _ins() -> InstrumentsConfig:
    return InstrumentsConfig(instruments=[InstrumentConfig(
        symbol="ESM6 Index", bloomberg_topic="ESM6 Index", exchange="CME",
        currency="USD", tick_size=0.25, point_value=50.0, min_qty=1, max_qty=50,
        roll_days_before_expiry=8, session_tz="America/Chicago",
        session_open="00:00", session_close="23:59",
    )])


def _intent(now: datetime) -> OrderIntent:
    return OrderIntent(
        strategy_id="s", instrument="ESM6 Index",
        side=Side.BUY, qty=1,
        order_type=OrderType.MKT, time_in_force=TimeInForce.DAY,
        idempotency_key="k1", source_timestamp=now,
    )


def test_rejected_intents_do_not_drain_rate_limit():
    now = datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc)
    clock = FixedClock(now)
    rate = RateLimiter(5, 60, clock=clock)
    gw = PreTradeRiskGateway(
        limits=RiskLimits(
            max_order_qty=10, max_position=20, max_notional=1e9,
            stale_data_seconds=60, require_market_session=False,
            max_orders_per_minute=5, max_cancels_per_minute=10,
        ),
        instruments=_ins(),
        positions=PositionBook(),
        exposure=ExposureCalculator(_ins()),
        stale_monitor=StaleDataMonitor(60, clock=clock),  # never seen a tick
        kill_switch=KillSwitch(armed=True, clock=clock),
        order_rate=rate,
        get_mark=lambda _s: 4500.0,
        clock=clock,
    )
    for _ in range(10):
        gw.validate(_intent(now))
    assert rate.current() == 0


def test_approved_intents_do_consume_rate_limit():
    now = datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc)
    clock = FixedClock(now)
    stale = StaleDataMonitor(60, clock=clock)
    # Mark fresh data so stale_data check passes.
    from futures_emsx_strategy.core.events import MarketTick
    stale.on_tick(MarketTick(instrument="ESM6 Index", bid=1, ask=2, last=1.5,
                             volume=1, exchange_timestamp=now, receive_timestamp=now))
    rate = RateLimiter(5, 60, clock=clock)
    gw = PreTradeRiskGateway(
        limits=RiskLimits(
            max_order_qty=10, max_position=20, max_notional=1e9,
            stale_data_seconds=60, require_market_session=False,
            max_orders_per_minute=5, max_cancels_per_minute=10,
        ),
        instruments=_ins(),
        positions=PositionBook(),
        exposure=ExposureCalculator(_ins()),
        stale_monitor=stale,
        kill_switch=KillSwitch(armed=True, clock=clock),
        order_rate=rate,
        get_mark=lambda _s: 4500.0,
        clock=clock,
    )
    for _ in range(3):
        result = gw.validate(_intent(now))
        assert result.decision.approved
    assert rate.current() == 3
