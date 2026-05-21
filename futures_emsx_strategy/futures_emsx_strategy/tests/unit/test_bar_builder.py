from datetime import datetime, timezone

from futures_emsx_strategy.core.events import MarketTick
from futures_emsx_strategy.market_data.bar_builder import MinuteBarBuilder


def _tick(price: float, ts: datetime, vol: int = 1) -> MarketTick:
    return MarketTick(
        instrument="ESM6 Index",
        bid=price - 0.25,
        ask=price + 0.25,
        last=price,
        volume=vol,
        exchange_timestamp=ts,
        receive_timestamp=ts,
    )


def test_minute_bar_closes_on_next_minute():
    bars = []
    bb = MinuteBarBuilder(interval_minutes=1)
    bb.on_bar(bars.append)
    t0 = datetime(2026, 5, 20, 14, 31, 5, tzinfo=timezone.utc)
    t1 = datetime(2026, 5, 20, 14, 31, 45, tzinfo=timezone.utc)
    t2 = datetime(2026, 5, 20, 14, 32, 10, tzinfo=timezone.utc)
    bb.on_tick(_tick(100.0, t0, 5))
    bb.on_tick(_tick(101.0, t1, 3))
    bb.on_tick(_tick(99.5, t2, 7))
    assert len(bars) == 1
    bar = bars[0]
    assert bar.open == 100.0
    assert bar.high == 101.0
    assert bar.low == 100.0
    assert bar.close == 101.0
    assert bar.volume == 8


def test_flush_emits_pending_bar():
    bars = []
    bb = MinuteBarBuilder(1)
    bb.on_bar(bars.append)
    bb.on_tick(_tick(100.0, datetime(2026, 5, 20, 14, 31, 5, tzinfo=timezone.utc)))
    bb.flush()
    assert len(bars) == 1
