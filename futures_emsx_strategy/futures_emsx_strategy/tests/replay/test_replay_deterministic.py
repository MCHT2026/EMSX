"""Replay determinism check: running the same bar series twice must produce identical orders."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from futures_emsx_strategy.core.events import BarClosed
from futures_emsx_strategy.orders.idempotency import IdempotencyStore
from futures_emsx_strategy.orders.models import WorkingOrderBook
from futures_emsx_strategy.orders.order_manager import OrderManager
from futures_emsx_strategy.portfolio.positions import PositionBook
from futures_emsx_strategy.strategy.minute_strategy import MinuteFuturesStrategy


def _make_bars(prices: list[float]) -> list[BarClosed]:
    t0 = datetime(2026, 5, 20, 14, 0, tzinfo=timezone.utc)
    return [
        BarClosed(
            instrument="ESM6 Index",
            start_time=t0 + timedelta(minutes=i),
            end_time=t0 + timedelta(minutes=i + 1),
            open=p,
            high=p,
            low=p,
            close=p,
            volume=1,
            interval_minutes=1,
        )
        for i, p in enumerate(prices)
    ]


def _run(bars):
    pos = PositionBook()
    om = OrderManager(pos, WorkingOrderBook(), IdempotencyStore())
    strat = MinuteFuturesStrategy(
        "s",
        "ESM6 Index",
        base_qty=2,
        params={"fast_lookback": 2, "slow_lookback": 4, "max_position_contracts": 6},
    )
    intents = []
    for bar in bars:
        for tgt in strat.on_bar(bar, pos):
            intents += om.on_target(tgt)
    return [(i.side.value, i.qty, i.idempotency_key) for i in intents]


def test_replay_is_deterministic():
    bars = _make_bars([100 + 0.5 * i for i in range(30)])
    assert _run(bars) == _run(bars)
