from datetime import datetime, timezone

from futures_emsx_strategy.core.clock import FixedClock
from futures_emsx_strategy.core.enums import OrderType, Side, TimeInForce
from futures_emsx_strategy.core.events import MarketTick, OrderIntent
from futures_emsx_strategy.market_data.stale_data_monitor import StaleDataMonitor
from futures_emsx_strategy.orders.throttles import RateLimiter
from futures_emsx_strategy.portfolio.exposure import ExposureCalculator
from futures_emsx_strategy.portfolio.positions import PositionBook
from futures_emsx_strategy.risk.kill_switch import KillSwitch
from futures_emsx_strategy.risk.limits import RiskLimits
from futures_emsx_strategy.risk.pre_trade import PreTradeRiskGateway


def _intent(qty: int, side: Side = Side.BUY) -> OrderIntent:
    return OrderIntent(
        strategy_id="minute_es_v1",
        instrument="ESM6 Index",
        side=side,
        qty=qty,
        order_type=OrderType.MKT,
        time_in_force=TimeInForce.DAY,
        idempotency_key="k1",
        source_timestamp=datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc),
    )


def _build(instruments_cfg, *, kill_armed=True, stale=False, position=0):
    now = datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc)
    clock = FixedClock(now)
    stale_mon = StaleDataMonitor(max_age_seconds=60, clock=clock)
    if not stale:
        stale_mon.on_tick(MarketTick(
            instrument="ESM6 Index",
            bid=4500.0, ask=4500.5, last=4500.25,
            volume=1, exchange_timestamp=now, receive_timestamp=now,
        ))
    pos = PositionBook()
    if position:
        pos.set("ESM6 Index", position)
    limits = RiskLimits(
        max_order_qty=10,
        max_position=20,
        max_notional=10_000_000.0,
        stale_data_seconds=60,
        require_market_session=False,
        max_orders_per_minute=1000,
        max_cancels_per_minute=1000,
    )
    gw = PreTradeRiskGateway(
        limits=limits,
        instruments=instruments_cfg,
        positions=pos,
        exposure=ExposureCalculator(instruments_cfg),
        stale_monitor=stale_mon,
        kill_switch=KillSwitch(armed=kill_armed, clock=clock),
        order_rate=RateLimiter(1000, 60, clock=clock),
        get_mark=lambda _s: 4500.0,
        clock=clock,
    )
    return gw, pos


def test_approves_normal_order(instruments_cfg):
    gw, _ = _build(instruments_cfg)
    assert gw.validate(_intent(5)).decision.approved


def test_rejects_when_over_max_order_qty(instruments_cfg):
    gw, _ = _build(instruments_cfg)
    result = gw.validate(_intent(100))
    assert not result.decision.approved
    assert any("max_order_qty" in r for r in result.decision.reasons)


def test_rejects_when_stale(instruments_cfg):
    gw, _ = _build(instruments_cfg, stale=True)
    result = gw.validate(_intent(5))
    assert not result.decision.approved
    assert any("stale_market_data" in r for r in result.decision.reasons)


def test_rejects_when_over_max_position(instruments_cfg):
    gw, _ = _build(instruments_cfg, position=18)
    result = gw.validate(_intent(5))
    assert not result.decision.approved
    assert any("max_position" in r for r in result.decision.reasons)
