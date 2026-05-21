from datetime import datetime, timezone

from futures_emsx_strategy.core.enums import OrderType, Side, TimeInForce
from futures_emsx_strategy.core.events import OrderIntent
from futures_emsx_strategy.execution.emsx_adapter import EMSXExecutionAdapter

from .fake_emsx import FakeEMSXRequests, FakeEMSXSubscriptions


def _intent() -> OrderIntent:
    return OrderIntent(
        strategy_id="minute_es_v1",
        instrument="ESM6 Index",
        side=Side.BUY,
        qty=3,
        order_type=OrderType.MKT,
        time_in_force=TimeInForce.DAY,
        idempotency_key="k1",
        source_timestamp=datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc),
    )


def test_submit_then_route_filled(emsx_cfg):
    requests = FakeEMSXRequests()
    subs = FakeEMSXSubscriptions()
    adapter = EMSXExecutionAdapter(
        config=emsx_cfg,
        auto_route=True,
        requests=requests,
        subscriptions=subs,
    )

    updates: list = []
    fills: list = []
    adapter.on_execution_update(updates.append)
    adapter.on_fill(fills.append)
    adapter.start()

    ack = adapter.submit_order(_intent())
    assert ack.accepted
    assert ack.order_id and ack.route_id

    subs.push_route({
        "EMSX_SEQUENCE": int(ack.order_id),
        "EMSX_ROUTE_ID": 1,
        "EMSX_TICKER": "ESM6 Index",
        "EMSX_SIDE": "BUY",
        "EMSX_AMOUNT": 3,
        "EMSX_STATUS": "WORKING",
        "EMSX_FILLED": 0,
        "EMSX_WORKING": 3,
    })
    subs.push_route({
        "EMSX_SEQUENCE": int(ack.order_id),
        "EMSX_ROUTE_ID": 1,
        "EMSX_TICKER": "ESM6 Index",
        "EMSX_SIDE": "BUY",
        "EMSX_AMOUNT": 3,
        "EMSX_STATUS": "FILLED",
        "EMSX_FILLED": 3,
        "EMSX_WORKING": 0,
        "EMSX_AVG_PRICE": 4500.25,
        "EMSX_FILL_AMOUNT": 3,
        "EMSX_FILL_PRICE": 4500.25,
    })
    assert len(updates) == 2
    assert updates[-1].status.value == "FILLED"
    assert len(fills) == 1
    assert fills[0].fill_qty == 3
