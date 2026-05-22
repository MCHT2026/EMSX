"""Tests for the EMSX gateway.

Uses the in-process mock BLPAPI session (tests.mocks.mock_blpapi). The
gateway must:

- consume orders.approved (and ONLY orders.approved)
- translate exec_style into EMSX_HAND_INSTRUCTION per the EXEC_STYLE_MAP
- preserve owner_id and message_id in EMSX_NOTES
- publish fills.partial / fills.done with the original owner_id recovered
  from EMSX_NOTES
- publish health.emsx.connected on SessionStarted, health.emsx.disconnected
  on SessionTerminated
"""
from __future__ import annotations

import asyncio
import json

import pytest

from modules.emsx_gateway import EmsxGateway, EXEC_STYLE_MAP
from tests.inmem_bus import InMemoryBus
from tests.mocks import mock_blpapi as blpapi


def _approved_envelope(**order_overrides) -> dict:
    base_order = {
        "order_id":   "o-1",
        "instrument": "ESH4 Index",
        "side":       "BUY",
        "qty":        10,
        "order_type": "LIMIT",
        "limit_price": 4850.25,
        "exec_style": "vwap",
        "broker":     "GSCO",
        "account":    "ACC001",
        "owner_id_source": "strategy",
        "approved_at":     "2026-05-22T12:00:00.000000Z",
    }
    base_order.update(order_overrides)
    return {
        "owner_id":   "risk_gate",
        "message_id": "m-1",
        "timestamp":  "2026-05-22T12:00:00.000000Z",
        "topic":      "orders.approved",
        "data":       base_order,
    }


async def _make_gateway(bus: InMemoryBus) -> EmsxGateway:
    await bus.connect()
    gw = EmsxGateway(bus=bus, blpapi_module=blpapi)
    await gw.open()
    return gw


# ---- exec_style mapping --------------------------------------------------

def test_exec_style_map_matches_spec():
    assert EXEC_STYLE_MAP["market"]["EMSX_HAND_INSTRUCTION"]        == "MKT"
    assert EXEC_STYLE_MAP["vwap"]["EMSX_HAND_INSTRUCTION"]          == "VWAP"
    assert EXEC_STYLE_MAP["twap"]["EMSX_HAND_INSTRUCTION"]          == "TWAP"
    assert EXEC_STYLE_MAP["passive_limit"]["EMSX_HAND_INSTRUCTION"] == "LIMIT"


# ---- translation ---------------------------------------------------------

async def test_handle_order_sends_emsx_fields():
    bus = InMemoryBus()
    gw = await _make_gateway(bus)

    env = _approved_envelope()
    await gw.handle_approved(env, "rid-1")
    await asyncio.sleep(0.05)

    sent = gw.session.sent_orders
    assert len(sent) == 1
    o = sent[0]
    assert o["EMSX_TICKER"]   == "ESH4 Index"
    assert o["EMSX_SIDE"]     == "BUY"
    assert o["EMSX_AMOUNT"]   == 10
    assert o["EMSX_ORDER_TYPE"] == "LIMIT"
    assert o["EMSX_BROKER"]   == "GSCO"
    assert o["EMSX_HAND_INSTRUCTION"] == "VWAP"  # exec_style="vwap"
    # owner_id_source + message_id encoded in EMSX_NOTES.
    notes = json.loads(o["EMSX_NOTES"])
    assert notes["owner_id_source"] == "strategy"
    assert notes["message_id"]      == "m-1"

    await gw.close()


# ---- fills round-trip ----------------------------------------------------

async def test_fills_published_with_original_owner_id():
    bus = InMemoryBus()
    gw = await _make_gateway(bus)

    partials: list[dict] = []
    dones:    list[dict] = []

    async def cap_partial(env, _mid): partials.append(env)
    async def cap_done(env, _mid):    dones.append(env)

    await bus.subscribe("fills.partial", "spy", "s1", cap_partial)
    await bus.subscribe("fills.done",    "spy", "s2", cap_done)

    env = _approved_envelope()
    await gw.handle_approved(env, "rid")
    # Give the mock's worker thread time to emit and the gateway's dispatch
    # loop time to forward.
    await asyncio.sleep(0.3)

    assert len(partials) == 1
    assert len(dones)    == 1
    assert partials[0]["data"]["owner_id_source"] == "strategy"
    assert dones[0]["data"]["owner_id_source"]    == "strategy"
    # filled_qty is the *increment* for this fill (spec example shows 5 of 5);
    # total_filled is cumulative. The mock emits a 50% partial then a fill,
    # so we get two increments of 5 totalling 10.
    assert partials[0]["data"]["filled_qty"]   == 5
    assert dones[0]["data"]["filled_qty"]      == 5
    assert dones[0]["data"]["total_filled"]    == 10
    assert dones[0]["data"]["avg_price"]   == 4850.25

    await gw.close()


# ---- session lifecycle ---------------------------------------------------

async def test_publishes_emsx_connected_on_session_started():
    bus = InMemoryBus()
    seen: list[dict] = []

    async def cap(env, _mid): seen.append(env)
    await bus.connect()
    await bus.subscribe("health.emsx.connected", "spy", "s", cap)

    gw = EmsxGateway(bus=bus, blpapi_module=blpapi)
    await gw.open()  # open() triggers session.start() -> SessionStarted event
    await asyncio.sleep(0.1)

    assert any(e["topic"] == "health.emsx.connected" for e in seen)
    await gw.close()


async def test_publishes_emsx_disconnected_on_session_terminated():
    bus = InMemoryBus()
    seen: list[dict] = []
    async def cap(env, _mid): seen.append(env)
    await bus.connect()
    await bus.subscribe("health.emsx.disconnected", "spy", "s", cap)

    gw = EmsxGateway(bus=bus, blpapi_module=blpapi)
    await gw.open()
    await gw.close()  # close stops the session -> SessionTerminated
    await asyncio.sleep(0.1)

    assert any(e["topic"] == "health.emsx.disconnected" for e in seen)


# ---- ignores everything except orders.approved --------------------------

async def test_only_consumes_orders_approved():
    """The gateway's run() registers ONE subscription topic: orders.approved.
    We exercise this at the API level — its subscription list should be
    exactly that.
    """
    bus = InMemoryBus()
    gw = await _make_gateway(bus)
    # Trigger run() to register subscriptions, then cancel.
    task = asyncio.create_task(gw.run())
    await asyncio.sleep(0.05)
    topics = {pat for pat, *_ in gw._subscriptions}
    assert topics == {"orders.approved"}
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    await gw.close()
