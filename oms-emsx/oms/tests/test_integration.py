"""Integration tests for spec-mandated end-to-end behaviors.

The spec ("Key test cases to cover") calls out scenarios that span multiple
modules:

- Duplicate message is acked and skipped without processing
- owner_id is preserved end-to-end from orders.new -> fills.done
- Kill switch blocks all orders

Wired together with the in-memory bus + mock BLPAPI so the whole pipeline
runs in-process.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from modules.archiver import Archiver
from modules.emsx_gateway import EmsxGateway
from modules.risk_gate import RiskGate
from tests.inmem_bus import InMemoryBus
from tests.mocks import mock_blpapi as blpapi


def _strategy_envelope(order_id: str = "o-1", message_id: str = "m-1") -> dict:
    return {
        "owner_id":   "strategy",
        "message_id": message_id,
        "timestamp":  "2026-05-22T12:00:00.000000Z",
        "topic":      "orders.new",
        "data": {
            "order_id":    order_id,
            "instrument":  "ESH4 Index",
            "side":        "BUY",
            "qty":         5,
            "order_type":  "LIMIT",
            "limit_price": 4850.25,
            "exec_style":  "vwap",
            "broker":      "GSCO",
            "account":     "ACC001",
        },
    }


async def _wire_pipeline(bus: InMemoryBus) -> tuple[RiskGate, EmsxGateway]:
    await bus.connect()

    gate = RiskGate(bus=bus)
    # Seed risk gate state so the order passes (qty=5).
    gate.state["prices"]["ESH4 Index"]                = 4850.25
    gate.state["volatility"]["ESH4 Index"]            = 0.01
    gate.state["positions"][("ACC001", "ESH4 Index")] = 0
    gate.state["margin"]["ACC001"]                    = 10_000_000

    gw = EmsxGateway(bus=bus, blpapi_module=blpapi)
    await gw.open()

    # Use BaseModule.subscribe (NOT bus.subscribe directly) so the wrapped
    # process_message runs — that's where idempotency + ack live.
    await gate.subscribe("orders.new",      gate.handle_order)
    await gw.subscribe(  "orders.approved", gw.handle_approved)
    return gate, gw


# ---- owner_id preservation end-to-end ------------------------------------

async def test_owner_id_preserved_from_orders_new_to_fills_done():
    bus = InMemoryBus()
    gate, gw = await _wire_pipeline(bus)

    fills_done: list[dict] = []
    async def cap(env, _mid): fills_done.append(env)
    await bus.subscribe("fills.done", "spy", "s", cap)

    await bus.publish("orders.new", _strategy_envelope())
    # Give the in-memory delivery + mock BLPAPI fills time to play out.
    await asyncio.sleep(0.5)

    assert len(fills_done) == 1
    done = fills_done[0]
    assert done["data"]["owner_id_source"] == "strategy"
    # The publisher of fills.done is the gateway, not the original strategy.
    assert done["owner_id"] == "emsx_gateway"

    await gw.close()


# ---- duplicate handling --------------------------------------------------

async def test_duplicate_orders_new_is_acked_and_skipped():
    """If the strategy re-publishes the same message_id (e.g. after a crash),
    the risk gate must process it exactly once."""
    bus = InMemoryBus()
    gate, gw = await _wire_pipeline(bus)

    approved: list[dict] = []
    async def cap(env, _mid): approved.append(env)
    await bus.subscribe("orders.approved", "spy", "s", cap)

    env = _strategy_envelope(order_id="o-dup", message_id="duplicate-mid")
    await bus.publish("orders.new", env)
    await asyncio.sleep(0.1)
    # Re-publish the SAME envelope -> idempotent dedup hits.
    await bus.publish("orders.new", env)
    await asyncio.sleep(0.1)

    # Exactly one approval despite two publishes.
    assert len(approved) == 1
    await gw.close()


# ---- kill switch blocks orders -------------------------------------------

async def test_kill_switch_blocks_all_orders():
    bus = InMemoryBus()
    gate, gw = await _wire_pipeline(bus)
    gate.kill_switch_active = True

    approved: list[dict] = []
    rejected: list[dict] = []
    async def capa(env, _mid): approved.append(env)
    async def capr(env, _mid): rejected.append(env)
    await bus.subscribe("orders.approved", "spy1", "s1", capa)
    await bus.subscribe("orders.rejected", "spy2", "s2", capr)

    await bus.publish("orders.new", _strategy_envelope(order_id="ka",
                                                       message_id="k1"))
    await bus.publish("orders.new", _strategy_envelope(order_id="kb",
                                                       message_id="k2"))
    await asyncio.sleep(0.2)

    assert approved == []
    assert len(rejected) == 2
    assert all(e["data"]["reason"] == "kill switch active" for e in rejected)
    await gw.close()
