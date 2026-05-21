"""Regression tests for the PositionBook avg-cost rules.

Covers cases the original ``test_positions_pnl`` did not assert: partial
close keeps avg_cost; reversal sets avg_cost to the fill price; full close
clears it.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from futures_emsx_strategy.core.enums import Side
from futures_emsx_strategy.core.events import FillUpdate
from futures_emsx_strategy.portfolio.positions import PositionBook


def _fill(side: Side, qty: int, px: float) -> FillUpdate:
    return FillUpdate(
        order_id="X",
        route_id=None,
        instrument="ESM6 Index",
        side=side,
        fill_qty=qty,
        fill_price=px,
        timestamp=datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc),
    )


def test_partial_close_keeps_avg_cost():
    b = PositionBook()
    b.apply_fill(_fill(Side.BUY, 10, 100.0))
    b.apply_fill(_fill(Side.SELL, 3, 110.0))
    assert b.position("ESM6 Index") == 7
    assert b.avg_cost("ESM6 Index") == pytest.approx(100.0)


def test_full_close_clears_avg_cost():
    b = PositionBook()
    b.apply_fill(_fill(Side.BUY, 5, 100.0))
    b.apply_fill(_fill(Side.SELL, 5, 110.0))
    assert b.position("ESM6 Index") == 0
    assert b.avg_cost("ESM6 Index") is None


def test_reversal_sets_avg_cost_to_fill_price():
    b = PositionBook()
    b.apply_fill(_fill(Side.BUY, 5, 100.0))
    b.apply_fill(_fill(Side.SELL, 10, 110.0))
    assert b.position("ESM6 Index") == -5
    assert b.avg_cost("ESM6 Index") == pytest.approx(110.0)


def test_stacked_long_takes_weighted_avg():
    b = PositionBook()
    b.apply_fill(_fill(Side.BUY, 5, 100.0))
    b.apply_fill(_fill(Side.BUY, 5, 102.0))
    assert b.position("ESM6 Index") == 10
    assert b.avg_cost("ESM6 Index") == pytest.approx(101.0)


def test_short_then_partial_cover_keeps_avg():
    b = PositionBook()
    b.apply_fill(_fill(Side.SELL, 8, 100.0))
    b.apply_fill(_fill(Side.BUY, 3, 95.0))
    assert b.position("ESM6 Index") == -5
    assert b.avg_cost("ESM6 Index") == pytest.approx(100.0)


def test_short_reversal_to_long_sets_avg_to_fill():
    b = PositionBook()
    b.apply_fill(_fill(Side.SELL, 4, 100.0))
    b.apply_fill(_fill(Side.BUY, 10, 95.0))
    assert b.position("ESM6 Index") == 6
    assert b.avg_cost("ESM6 Index") == pytest.approx(95.0)
