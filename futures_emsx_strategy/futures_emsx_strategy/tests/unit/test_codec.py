"""Round-trip serialization checks for the bus codec.

Reproduces the issue where Kafka/Redis serialized dataclasses to JSON dicts
and delivered the raw dicts to handlers expecting typed events.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

# Import topics to trigger topic-type registration.
import futures_emsx_strategy.app.topics as T
from futures_emsx_strategy.core.enums import OrderStatus, OrderType, Side, TimeInForce
from futures_emsx_strategy.core.events import (
    BarClosed,
    ExecutionUpdate,
    FillUpdate,
    OrderIntent,
    TargetPosition,
)
from futures_emsx_strategy.messaging.codec import decode, encode, register_topic


def _roundtrip(topic: str, payload):
    return decode(topic, json.loads(encode(payload)))


def test_bar_closed_roundtrip():
    bar = BarClosed(
        instrument="ESM6 Index",
        start_time=datetime(2026, 5, 20, 14, 30, tzinfo=timezone.utc),
        end_time=datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.5,
        close=100.5,
        volume=42,
        interval_minutes=1,
    )
    back = _roundtrip(T.BARS, bar)
    assert isinstance(back, BarClosed)
    assert back.instrument == "ESM6 Index"
    assert back.close == 100.5
    assert back.start_time == bar.start_time


def test_order_intent_roundtrip_preserves_enums():
    intent = OrderIntent(
        strategy_id="s",
        instrument="ESM6 Index",
        side=Side.BUY,
        qty=3,
        order_type=OrderType.MKT,
        time_in_force=TimeInForce.DAY,
        idempotency_key="k1",
        source_timestamp=datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc),
        limit_price=None,
        metadata={"reason": "signal_long"},
    )
    back = _roundtrip(T.INTENTS, intent)
    assert isinstance(back, OrderIntent)
    assert back.side is Side.BUY
    assert back.order_type is OrderType.MKT
    assert back.time_in_force is TimeInForce.DAY
    assert back.limit_price is None
    assert back.metadata == {"reason": "signal_long"}


def test_execution_update_roundtrip_preserves_status_enum():
    upd = ExecutionUpdate(
        order_id="k1",
        route_id="k1",
        instrument="ESM6 Index",
        status=OrderStatus.PART_FILLED,
        filled_qty=2,
        avg_price=4500.25,
        leaves_qty=1,
        timestamp=datetime(2026, 5, 20, 14, 31, 30, tzinfo=timezone.utc),
        raw={},
    )
    back = _roundtrip(T.EXECUTION_UPDATES, upd)
    assert isinstance(back, ExecutionUpdate)
    assert back.status is OrderStatus.PART_FILLED


def test_fill_update_roundtrip():
    fill = FillUpdate(
        order_id="k1",
        route_id="k1",
        instrument="ESM6 Index",
        side=Side.SELL,
        fill_qty=2,
        fill_price=4500.25,
        timestamp=datetime(2026, 5, 20, 14, 31, 30, tzinfo=timezone.utc),
    )
    back = _roundtrip(T.FILLS, fill)
    assert isinstance(back, FillUpdate)
    assert back.side is Side.SELL


def test_target_position_roundtrip():
    tgt = TargetPosition(
        strategy_id="s",
        instrument="ESM6 Index",
        target_qty=-3,
        timestamp=datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc),
        reason="signal_short=-1.0",
        metadata={"signal": -1.0},
    )
    back = _roundtrip(T.TARGETS, tgt)
    assert isinstance(back, TargetPosition)
    assert back.target_qty == -3


def test_decode_passthrough_when_topic_unregistered():
    assert decode("nope", {"a": 1}) == {"a": 1}
    assert decode("nope", 42) == 42


def test_register_topic_rejects_conflicting_class():
    register_topic("conflict_test_topic", BarClosed)
    with pytest.raises(ValueError):
        register_topic("conflict_test_topic", FillUpdate)
    register_topic("conflict_test_topic", BarClosed)  # idempotent rebind OK
