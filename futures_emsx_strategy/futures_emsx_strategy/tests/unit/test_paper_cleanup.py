"""Regression: paper fills must clear the working-order book.

Reproduces the bug where PaperExecutionAdapter fired SENT/FILL/FILLED callbacks
synchronously inside ``submit_order``, before the runner had a chance to
register the order. The fix is the order-id convention (idempotency_key as
internal id) and a runner pre-register-then-submit pattern.
"""
from __future__ import annotations

from datetime import datetime, timezone

from futures_emsx_strategy.core.enums import (
    OrderStatus,
    OrderType,
    Side,
    TERMINAL_ORDER_STATUSES,
    TimeInForce,
)
from futures_emsx_strategy.core.events import OrderIntent
from futures_emsx_strategy.execution.paper_adapter import PaperExecutionAdapter
from futures_emsx_strategy.orders.lifecycle import OrderLifecycle, OrderRecord
from futures_emsx_strategy.orders.models import WorkingOrderBook
from futures_emsx_strategy.portfolio.positions import PositionBook


def _intent(qty: int = 3, key: str = "k1") -> OrderIntent:
    return OrderIntent(
        strategy_id="s",
        instrument="ESM6 Index",
        side=Side.BUY,
        qty=qty,
        order_type=OrderType.MKT,
        time_in_force=TimeInForce.DAY,
        idempotency_key=key,
        source_timestamp=datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc),
    )


def _build():
    working = WorkingOrderBook()
    lifecycle = OrderLifecycle()
    positions = PositionBook()

    adapter = PaperExecutionAdapter(
        get_mark=lambda _s: 4500.0,
        tick_size_lookup=lambda _s: 0.25,
    )

    def on_upd(u):
        rec = lifecycle.resolve(u.order_id)
        if rec is None:
            return
        lifecycle.update_status(rec.order_id, u.status, filled_qty=u.filled_qty)
        working.upsert(rec.order_id, u.instrument, rec.side, u.leaves_qty, u.status)

    def on_fill(f):
        positions.apply_fill(f)

    adapter.on_execution_update(on_upd)
    adapter.on_fill(on_fill)
    adapter.start()
    return adapter, working, lifecycle, positions


def _pre_register(working, lifecycle, intent: OrderIntent) -> str:
    iid = intent.idempotency_key
    working.upsert(iid, intent.instrument, intent.side, intent.qty, OrderStatus.NEW)
    lifecycle.register(
        OrderRecord(
            order_id=iid,
            strategy_id=intent.strategy_id,
            instrument=intent.instrument,
            side=intent.side,
            qty=intent.qty,
            idempotency_key=intent.idempotency_key,
            created_at=datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc),
            status=OrderStatus.NEW,
        )
    )
    return iid


def test_working_book_empty_after_paper_fill():
    adapter, working, lifecycle, positions = _build()
    intent = _intent(qty=3)
    iid = _pre_register(working, lifecycle, intent)
    ack = adapter.submit_order(intent)
    assert ack.accepted
    assert ack.order_id == iid
    assert working.snapshot() == []
    assert working.net_working_qty("ESM6 Index") == 0
    rec = lifecycle.get(iid)
    assert rec is not None
    assert rec.status is OrderStatus.FILLED
    assert rec.status in TERMINAL_ORDER_STATUSES
    assert positions.position("ESM6 Index") == 3


def test_rejected_paper_order_clears_working_book():
    """submit_order with no mark must produce a REJECTED update so the
    pre-registered working entry doesn't linger."""
    adapter = PaperExecutionAdapter(get_mark=lambda _s: None)
    working = WorkingOrderBook()
    lifecycle = OrderLifecycle()

    def on_upd(u):
        rec = lifecycle.resolve(u.order_id)
        if rec is None:
            return
        working.upsert(rec.order_id, u.instrument, rec.side, u.leaves_qty, u.status)

    adapter.on_execution_update(on_upd)
    adapter.start()

    intent = _intent(qty=2, key="k2")
    _pre_register(working, lifecycle, intent)
    ack = adapter.submit_order(intent)
    assert not ack.accepted
    assert working.snapshot() == []


def test_double_submit_with_same_key_is_idempotent_at_adapter_level():
    """The adapter itself uses idempotency_key as the order_id; callers should
    rely on IdempotencyStore for dedupe, but at minimum two submits with the
    same intent produce two ack objects with the same order_id."""
    adapter, working, lifecycle, _positions = _build()
    intent = _intent(qty=1)
    _pre_register(working, lifecycle, intent)
    ack1 = adapter.submit_order(intent)
    ack2 = adapter.submit_order(intent)
    assert ack1.order_id == ack2.order_id == intent.idempotency_key
