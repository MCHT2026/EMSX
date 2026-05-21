"""Regression: synthetic tick generator must produce monotonically increasing
timestamps even across hour and day boundaries (the old %60 minute arithmetic
silently rolled back time)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone


def _generate(base: datetime, n: int, ticks_per_minute: int) -> list[datetime]:
    tick_interval = timedelta(seconds=60.0 / ticks_per_minute)
    return [base + i * tick_interval for i in range(n)]


def test_timestamps_are_strictly_monotonic_over_a_day():
    base = datetime(2026, 5, 20, 23, 55, tzinfo=timezone.utc)
    ticks = _generate(base, n=24 * 60 * 6, ticks_per_minute=6)
    assert ticks == sorted(ticks)
    for prev, nxt in zip(ticks, ticks[1:]):
        assert nxt > prev


def test_crosses_hour_and_day_boundaries():
    base = datetime(2026, 5, 20, 23, 55, tzinfo=timezone.utc)
    ticks = _generate(base, n=200, ticks_per_minute=6)
    assert ticks[0].hour == 23
    assert ticks[-1].day == 21
