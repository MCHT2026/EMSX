"""Regression: EMSX subscription updates must reach the runner even when the
route message arrives BEFORE ``submit_order`` returns the venue id."""
from __future__ import annotations

from datetime import datetime, timezone

from futures_emsx_strategy.config.loader import EMSXConfig
from futures_emsx_strategy.core.enums import OrderStatus, OrderType, Side, TimeInForce
from futures_emsx_strategy.core.events import OrderIntent
from futures_emsx_strategy.execution.emsx_adapter import EMSXExecutionAdapter
from futures_emsx_strategy.orders.lifecycle import OrderLifecycle, OrderRecord
from futures_emsx_strategy.tests.emsx_sim.fake_emsx import (
    FakeEMSXRequests,
    FakeEMSXSubscriptions,
)


def _intent(key: str = "k1") -> OrderIntent:
    return OrderIntent(
        strategy_id="s",
        instrument="ESM6 Index",
        side=Side.BUY,
        qty=3,
        order_type=OrderType.MKT,
        time_in_force=TimeInForce.DAY,
        idempotency_key=key,
        source_timestamp=datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc),
    )


def _register(lifecycle: OrderLifecycle, intent: OrderIntent) -> None:
    lifecycle.register(
        OrderRecord(
            order_id=intent.idempotency_key,
            strategy_id=intent.strategy_id,
            instrument=intent.instrument,
            side=intent.side,
            qty=intent.qty,
            idempotency_key=intent.idempotency_key,
            created_at=datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc),
            status=OrderStatus.NEW,
        )
    )


def _build():
    cfg = EMSXConfig(broker="BMTB", account="TEST")
    req = FakeEMSXRequests()
    subs = FakeEMSXSubscriptions()
    adapter = EMSXExecutionAdapter(config=cfg, auto_route=True, requests=req, subscriptions=subs)
    lifecycle = OrderLifecycle()
    updates: list = []
    fills: list = []

    def on_upd(u):
        rec = lifecycle.resolve(u.order_id)
        updates.append((u.order_id, u.status, "matched" if rec else "DROPPED"))

    def on_fill(f):
        rec = lifecycle.resolve(f.order_id)
        fills.append((f.order_id, f.fill_qty, "matched" if rec else "DROPPED"))

    adapter.on_execution_update(on_upd)
    adapter.on_fill(on_fill)
    adapter.start()
    return adapter, subs, req, lifecycle, updates, fills


def test_update_arriving_before_ack_is_buffered_then_replayed():
    adapter, subs, req, lifecycle, updates, fills = _build()
    intent = _intent("k1")
    _register(lifecycle, intent)

    # FakeEMSXRequests allocates EMSX_SEQUENCE starting at 1000.
    seq = 1000
    # Push the route update FIRST, before submit_order.
    subs.push_route({
        "EMSX_SEQUENCE": seq, "EMSX_ROUTE_ID": 1,
        "EMSX_TICKER": "ESM6 Index", "EMSX_SIDE": "BUY",
        "EMSX_AMOUNT": 3, "EMSX_STATUS": "WORKING",
        "EMSX_FILLED": 0, "EMSX_WORKING": 3,
    })
    # Nothing dispatched yet; buffer holds the update.
    assert updates == []

    # submit_order returns ack with EMSX_SEQUENCE=1000 -> mapping populated;
    # buffered update is replayed with order_id rewritten to the client id.
    ack = adapter.submit_order(intent)
    assert ack.accepted

    assert len(updates) == 1
    order_id, status, resolved = updates[0]
    assert order_id == "k1"
    assert status is OrderStatus.WORKING
    assert resolved == "matched"


def test_subsequent_subscriptions_are_dispatched_with_client_id():
    adapter, subs, req, lifecycle, updates, fills = _build()
    intent = _intent("k2")
    _register(lifecycle, intent)
    ack = adapter.submit_order(intent)
    seq = int(ack.order_id)

    # Now updates flow normally; order_id should be rewritten to the client id.
    subs.push_route({
        "EMSX_SEQUENCE": seq, "EMSX_ROUTE_ID": 1,
        "EMSX_TICKER": "ESM6 Index", "EMSX_SIDE": "BUY",
        "EMSX_AMOUNT": 3, "EMSX_STATUS": "FILLED",
        "EMSX_FILLED": 3, "EMSX_WORKING": 0,
        "EMSX_AVG_PRICE": 4500.25,
        "EMSX_FILL_AMOUNT": 3, "EMSX_FILL_PRICE": 4500.25,
    })

    assert any(u[0] == "k2" and u[1] is OrderStatus.FILLED and u[2] == "matched"
               for u in updates)
    assert any(f[0] == "k2" and f[1] == 3 and f[2] == "matched" for f in fills)
