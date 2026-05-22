"""End-to-end smoke demo of the OMS EMSX pipeline.

What this does (no Redis, no Bloomberg required):

1. Spins up an in-process event bus.
2. Wires risk_gate + emsx_gateway + archiver, with the mock BLPAPI session.
3. Publishes three synthetic orders:
     - one that PASSES every risk check  -> ends as fills.done
     - one that BREACHES notional        -> orders.rejected
     - one with the KILL SWITCH active   -> orders.rejected
4. Subscribes a "tracer" that logs every message on every topic and shows
   the journey: orders.new -> orders.approved -> fills.partial -> fills.done.

Run::

    python scripts/smoke_demo.py
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

# Make sibling packages importable when run as a script.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from modules.archiver import Archiver
from modules.emsx_gateway import EmsxGateway
from modules.risk_gate import RiskGate
from tests.inmem_bus import InMemoryBus
from tests.mocks import mock_blpapi as blpapi


def fmt(env: dict) -> str:
    d = env.get("data", {})
    interesting = {k: d[k] for k in
                   ("order_id", "reason", "filled_qty", "total_filled",
                    "avg_price", "owner_id_source") if k in d}
    return f"  [{env['owner_id']:>13}] {env['topic']:<28} {interesting}"


async def main() -> None:
    bus = InMemoryBus()
    await bus.connect()

    # ----- wire modules -------------------------------------------------
    archiver = Archiver(bus=bus,
                        log_dir=str(ROOT / "logs"),
                        db_path=str(ROOT / "logs" / "smoke_demo.sqlite"))
    await archiver.open()
    await archiver.subscribe("*", archiver.handle_message)

    risk_gate = RiskGate(bus=bus)
    risk_gate.state["prices"]["ESH4 Index"]                = 4850.25
    risk_gate.state["volatility"]["ESH4 Index"]            = 0.01
    risk_gate.state["positions"][("ACC001", "ESH4 Index")] = 0
    risk_gate.state["margin"]["ACC001"]                    = 10_000_000
    await risk_gate.subscribe("orders.new",     risk_gate.handle_order)
    await risk_gate.subscribe("orders.rejected", risk_gate.track_rejection)

    gateway = EmsxGateway(bus=bus, blpapi_module=blpapi)
    await gateway.open()
    await gateway.subscribe("orders.approved", gateway.handle_approved)

    # ----- tracer: print every message on every topic -------------------
    async def trace(env: dict, _mid: str) -> None:
        print(fmt(env))

    await bus.subscribe("*", "tracer", "tracer:1", trace)

    print("\n--- 1. happy-path order (qty=5, all checks pass) ---")
    await bus.publish("orders.new", {
        "owner_id": "strategy", "message_id": "m-happy",
        "timestamp": "2026-05-22T12:00:00.000000Z", "topic": "orders.new",
        "data": {
            "order_id": "o-happy", "instrument": "ESH4 Index",
            "side": "BUY", "qty": 5, "order_type": "LIMIT",
            "limit_price": 4850.25, "exec_style": "vwap",
            "broker": "GSCO", "account": "ACC001",
        },
    })
    await asyncio.sleep(0.6)  # let fills complete

    print("\n--- 2. notional breach (qty=50,000 > 1M USD notional cap) ---")
    await bus.publish("orders.new", {
        "owner_id": "strategy", "message_id": "m-big",
        "timestamp": "2026-05-22T12:00:01.000000Z", "topic": "orders.new",
        "data": {
            "order_id": "o-big", "instrument": "ESH4 Index",
            "side": "BUY", "qty": 50_000, "order_type": "LIMIT",
            "limit_price": 4850.25, "exec_style": "market",
            "broker": "GSCO", "account": "ACC001",
        },
    })
    await asyncio.sleep(0.2)

    print("\n--- 3. kill switch ON (any order rejected) ---")
    risk_gate.kill_switch_active = True
    await bus.publish("orders.new", {
        "owner_id": "strategy", "message_id": "m-kill",
        "timestamp": "2026-05-22T12:00:02.000000Z", "topic": "orders.new",
        "data": {
            "order_id": "o-kill", "instrument": "ESH4 Index",
            "side": "BUY", "qty": 1, "order_type": "LIMIT",
            "limit_price": 4850.25, "exec_style": "market",
            "broker": "GSCO", "account": "ACC001",
        },
    })
    await asyncio.sleep(0.2)

    # ----- shutdown + show archive ----------------------------------------
    await archiver.flush()
    print("\n--- archive contents (per owner_id_source via Archiver.replay) ---")
    async for env in archiver.replay("strategy", "2026-05-22T00:00:00.000000Z"):
        print(f"  [archive] {env['topic']:<28} order={env['data'].get('order_id')}")

    await gateway.close()
    await archiver.close()
    await bus.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
