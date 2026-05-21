from datetime import datetime, timezone

from futures_emsx_strategy.core.enums import Side
from futures_emsx_strategy.core.events import FillUpdate
from futures_emsx_strategy.portfolio.pnl import PnLCalculator
from futures_emsx_strategy.portfolio.positions import PositionBook


def _fill(side: Side, qty: int, px: float) -> FillUpdate:
    return FillUpdate(
        order_id="A",
        route_id=None,
        instrument="ESM6 Index",
        side=side,
        fill_qty=qty,
        fill_price=px,
        timestamp=datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc),
    )


def test_position_basic(instruments_cfg):
    book = PositionBook()
    book.apply_fill(_fill(Side.BUY, 5, 4500.0))
    assert book.position("ESM6 Index") == 5
    assert book.avg_cost("ESM6 Index") == 4500.0
    book.apply_fill(_fill(Side.BUY, 5, 4501.0))
    assert book.position("ESM6 Index") == 10
    assert abs(book.avg_cost("ESM6 Index") - 4500.5) < 1e-9
    book.apply_fill(_fill(Side.SELL, 4, 4505.0))
    assert book.position("ESM6 Index") == 6


def test_pnl_closes_realize(instruments_cfg):
    pnl = PnLCalculator(instruments_cfg)
    pnl.apply_fill(_fill(Side.BUY, 5, 4500.0))
    pnl.apply_fill(_fill(Side.SELL, 5, 4502.0))
    assert pnl.realized() == 5 * 2.0 * 50.0
    assert pnl.unrealized({"ESM6 Index": 9999.0}) == 0.0
