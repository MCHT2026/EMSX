"""Tests for the Risk Gate module.

The risk gate is the only component allowed to publish ``orders.approved``.
Covered:

- happy path: all checks pass -> publishes orders.approved with original
  payload + ``approved_at`` timestamp, original owner_id preserved
- each check rejects independently (fail-fast): notional, position,
  margin, vol, kill switch
- rejections publish ``orders.rejected`` with the original owner_id and a
  reason field
- internal state updates when market.price.*, market.vol.*,
  positions.update, account.margin messages arrive
- rejection-spike alert fires once the rolling-window threshold is
  exceeded
"""
from __future__ import annotations

import asyncio

import pytest

from config import settings
from modules.risk_gate import RiskGate
from tests.inmem_bus import InMemoryBus


def _order(**overrides) -> dict:
    base = {
        "order_id":   "o-1",
        "instrument": "ESH4 Index",
        "side":       "BUY",
        "qty":        10,
        "order_type": "LIMIT",
        "limit_price": 4850.25,
        "exec_style": "vwap",
        "broker":     "GSCO",
        "account":    "ACC001",
    }
    base.update(overrides)
    return base


def _envelope(payload: dict, *, owner_id: str = "strategy",
              message_id: str = "m-1", topic: str = "orders.new") -> dict:
    return {
        "owner_id":   owner_id,
        "message_id": message_id,
        "timestamp":  "2026-05-22T12:00:00.000000Z",
        "topic":      topic,
        "data":       payload,
    }


async def _new_gate(bus: InMemoryBus) -> RiskGate:
    await bus.connect()
    gate = RiskGate(bus=bus)
    # Seed enough state for the happy path.
    gate.state["prices"]["ESH4 Index"]    = 4850.25
    gate.state["volatility"]["ESH4 Index"] = 0.01
    gate.state["positions"][("ACC001", "ESH4 Index")] = 0
    gate.state["margin"]["ACC001"] = 10_000_000
    return gate


# ---- happy path ----------------------------------------------------------

async def test_approves_when_all_checks_pass():
    bus = InMemoryBus()
    gate = await _new_gate(bus)

    approved: list[dict] = []
    rejected: list[dict] = []

    async def cap_approved(env, _mid): approved.append(env)
    async def cap_rejected(env, _mid): rejected.append(env)

    await bus.subscribe("orders.approved", "spy", "s", cap_approved)
    await bus.subscribe("orders.rejected", "spy2", "s2", cap_rejected)

    await gate.handle_order(_envelope(_order()), "rid-1")
    await asyncio.sleep(0.02)

    assert len(approved) == 1
    assert len(rejected) == 0
    env = approved[0]
    assert env["owner_id"] == "risk_gate"           # publisher
    assert env["data"]["owner_id_source"] == "strategy"
    assert env["data"]["order_id"] == "o-1"
    assert "approved_at" in env["data"]


# ---- rejection cases (fail-fast order) -----------------------------------

async def test_rejects_when_notional_exceeded():
    bus = InMemoryBus()
    gate = await _new_gate(bus)
    rejections: list[dict] = []

    async def cap(env, _mid): rejections.append(env)
    await bus.subscribe("orders.rejected", "spy", "s", cap)

    # qty * price = 50,000 * 4850.25 >> default MAX_NOTIONAL 1_000_000
    await gate.handle_order(_envelope(_order(qty=50_000)), "rid")
    await asyncio.sleep(0.02)

    assert len(rejections) == 1
    env = rejections[0]
    assert env["data"]["reason"] == "notional limit breached"
    assert env["data"]["owner_id_source"] == "strategy"
    assert env["data"]["order_id"] == "o-1"


async def test_rejects_when_position_limit_breached():
    bus = InMemoryBus()
    gate = await _new_gate(bus)
    # Default MAX_POSITION is 100; start at 95 + 10 -> 105 > 100.
    gate.state["positions"][("ACC001", "ESH4 Index")] = 95
    rejections: list[dict] = []
    async def cap(env, _mid): rejections.append(env)
    await bus.subscribe("orders.rejected", "spy", "s", cap)

    await gate.handle_order(_envelope(_order()), "rid")
    await asyncio.sleep(0.02)

    assert rejections[0]["data"]["reason"] == "position limit breached"


async def test_rejects_when_margin_insufficient():
    bus = InMemoryBus()
    gate = await _new_gate(bus)
    gate.state["margin"]["ACC001"] = 100.0  # tiny
    rejections: list[dict] = []
    async def cap(env, _mid): rejections.append(env)
    await bus.subscribe("orders.rejected", "spy", "s", cap)

    await gate.handle_order(_envelope(_order()), "rid")
    await asyncio.sleep(0.02)

    assert rejections[0]["data"]["reason"] == "margin insufficient"


async def test_rejects_when_vol_above_threshold():
    bus = InMemoryBus()
    gate = await _new_gate(bus)
    gate.state["volatility"]["ESH4 Index"] = 0.2  # above default 0.05
    rejections: list[dict] = []
    async def cap(env, _mid): rejections.append(env)
    await bus.subscribe("orders.rejected", "spy", "s", cap)

    await gate.handle_order(_envelope(_order()), "rid")
    await asyncio.sleep(0.02)

    assert rejections[0]["data"]["reason"] == "volatility above threshold"


async def test_rejects_when_kill_switch_active():
    bus = InMemoryBus()
    gate = await _new_gate(bus)
    gate.kill_switch_active = True
    rejections: list[dict] = []
    async def cap(env, _mid): rejections.append(env)
    await bus.subscribe("orders.rejected", "spy", "s", cap)

    await gate.handle_order(_envelope(_order()), "rid")
    await asyncio.sleep(0.02)

    assert rejections[0]["data"]["reason"] == "kill switch active"


# ---- state updates from market / position / margin streams ---------------

async def test_state_updates_from_market_streams():
    bus = InMemoryBus()
    gate = await _new_gate(bus)

    await gate.handle_market_price(
        _envelope({"instrument": "ZNH4 Comdty", "last": 110.5,
                   "bid": 110.45, "ask": 110.55}, topic="market.price.ZNH4"),
        "rid",
    )
    assert gate.state["prices"]["ZNH4 Comdty"] == 110.5

    await gate.handle_market_vol(
        _envelope({"instrument": "ZNH4 Comdty", "vol": 0.08},
                  topic="market.vol.ZNH4"),
        "rid",
    )
    assert gate.state["volatility"]["ZNH4 Comdty"] == 0.08

    await gate.handle_position_update(
        _envelope({"account": "ACC001", "instrument": "ZNH4 Comdty",
                   "net": 5, "gross": 5, "avg_cost": 110.0},
                  topic="positions.update"),
        "rid",
    )
    assert gate.state["positions"][("ACC001", "ZNH4 Comdty")] == 5

    await gate.handle_margin_update(
        _envelope({"account": "ACC001", "available": 1_500_000},
                  topic="account.margin"),
        "rid",
    )
    assert gate.state["margin"]["ACC001"] == 1_500_000


# ---- rejection-spike alert -----------------------------------------------

async def test_rejection_spike_alert_fires():
    bus = InMemoryBus()
    gate = await _new_gate(bus)
    # Tiny threshold for the test.
    gate.rejection_threshold = 3
    alerts: list[dict] = []
    async def cap(env, _mid): alerts.append(env)
    await bus.subscribe("health.risk.rejection_spike", "spy", "s", cap)

    # Track 4 rejections from same owner_id (already-rejected feed).
    for i in range(4):
        await gate.track_rejection(
            _envelope({"reason": "x", "owner_id_source": "strategy"},
                      message_id=f"r-{i}", topic="orders.rejected"),
            f"rid-{i}",
        )
    await asyncio.sleep(0.02)
    assert len(alerts) >= 1
    assert alerts[-1]["data"]["owner_id"] == "strategy"
    assert alerts[-1]["data"]["count"] >= 3
