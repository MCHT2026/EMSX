"""Bar invariants: end_time > start_time, regardless of source."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from futures_emsx_strategy.core.events import BarClosed
from futures_emsx_strategy.market_data.mock_provider import MockMarketDataProvider


def test_mock_historical_bars_have_strictly_later_end():
    p = MockMarketDataProvider()
    start = datetime(2026, 5, 20, 14, 0, tzinfo=timezone.utc)
    end = datetime(2026, 5, 20, 14, 5, tzinfo=timezone.utc)
    bars = list(p.request_historical_bars("ESM6 Index", start, end, interval_minutes=1))
    assert bars, "mock provider should produce bars"
    for b in bars:
        assert b.end_time > b.start_time
        assert b.end_time - b.start_time == timedelta(minutes=1)


def test_bar_closed_dataclass_is_constructed_with_separate_times():
    # Encodes the invariant from contracts.tex: end_time > start_time.
    bar = BarClosed(
        instrument="ESM6 Index",
        start_time=datetime(2026, 5, 20, 14, 30, tzinfo=timezone.utc),
        end_time=datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc),
        open=100, high=101, low=99, close=100.5, volume=1,
    )
    assert bar.end_time > bar.start_time
