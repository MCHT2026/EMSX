from datetime import datetime, timezone

from futures_emsx_strategy.core.enums import OrderType, Side, TimeInForce
from futures_emsx_strategy.core.events import TargetPosition
from futures_emsx_strategy.orders.idempotency import IdempotencyStore
from futures_emsx_strategy.orders.models import WorkingOrderBook
from futures_emsx_strategy.orders.order_manager import OrderManager
from futures_emsx_strategy.portfolio.positions import PositionBook


def _target(qty: int, ts: datetime) -> TargetPosition:
    return TargetPosition(
        strategy_id="minute_es_v1",
        instrument="ESM6 Index",
        target_qty=qty,
        timestamp=ts,
        reason="t",
    )


def test_basic_buy_delta():
    pos = PositionBook()
    pos.set("ESM6 Index", 5)
    om = OrderManager(pos, WorkingOrderBook(), IdempotencyStore())
    intents = om.on_target(_target(8, datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc)))
    assert len(intents) == 1
    assert intents[0].side is Side.BUY
    assert intents[0].qty == 3
    assert intents[0].order_type is OrderType.MKT
    assert intents[0].time_in_force is TimeInForce.DAY


def test_no_order_when_target_equals_position():
    pos = PositionBook()
    pos.set("ESM6 Index", 8)
    om = OrderManager(pos, WorkingOrderBook(), IdempotencyStore())
    intents = om.on_target(_target(8, datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc)))
    assert intents == []


def test_working_qty_offsets_delta():
    pos = PositionBook()
    pos.set("ESM6 Index", 5)
    working = WorkingOrderBook()
    from futures_emsx_strategy.core.enums import OrderStatus
    working.upsert("X1", "ESM6 Index", Side.BUY, 3, OrderStatus.WORKING)
    om = OrderManager(pos, working, IdempotencyStore())
    intents = om.on_target(_target(8, datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc)))
    assert intents == []


def test_short_delta():
    pos = PositionBook()
    pos.set("ESM6 Index", 5)
    om = OrderManager(pos, WorkingOrderBook(), IdempotencyStore())
    intents = om.on_target(_target(-3, datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc)))
    assert intents[0].side is Side.SELL
    assert intents[0].qty == 8


def test_idempotency_seen_suppresses_regeneration():
    """OrderManager regenerates intents on every call (because risk may have
    rejected the previous one). Once the runner *commits* a key via
    ``IdempotencyStore.mark`` (or .claim), subsequent regeneration is
    suppressed."""
    pos = PositionBook()
    pos.set("ESM6 Index", 5)
    idemp = IdempotencyStore()
    om = OrderManager(pos, WorkingOrderBook(), idemp)
    ts = datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc)

    first = om.on_target(_target(8, ts))
    assert len(first) == 1
    # Transient rejection scenario: nothing marked -> next call still emits.
    again_unmarked = om.on_target(_target(8, ts))
    assert len(again_unmarked) == 1, "transient rejection must remain retryable"
    # Runner committed the key (e.g. successful submission). Now suppress.
    idemp.mark(first[0].idempotency_key)
    third = om.on_target(_target(8, ts))
    assert third == []
