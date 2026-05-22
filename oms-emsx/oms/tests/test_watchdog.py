"""Tests for the Watchdog.

Covered:
- a heartbeat from an unknown module registers it as alive
- a known module that misses heartbeats long enough is marked degraded and
  a restart is attempted
- restart attempts are capped at WATCHDOG_MAX_RESTARTS
- previously-degraded modules that resume heartbeating publish health.restored
- PEL > threshold publishes health.pel_growing
- Sentinel +switch-master triggers health.bus.failover
"""
from __future__ import annotations

import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest

from modules.watchdog import Watchdog
from tests.inmem_bus import InMemoryBus


def _heartbeat_envelope(owner_id: str, pid: int = 12345) -> dict:
    return {
        "owner_id":   owner_id,
        "message_id": f"hb-{owner_id}-{time.time_ns()}",
        "timestamp":  "2026-05-22T12:00:00.000000Z",
        "topic":      "health.heartbeat",
        "data":       {"owner_id": owner_id, "pid": pid, "status": "alive"},
    }


async def _new_watchdog(bus: InMemoryBus, **overrides) -> Watchdog:
    await bus.connect()
    wd = Watchdog(
        bus=bus,
        known_modules=overrides.get("known_modules", {"risk_gate": "modules/risk_gate.py"}),
        heartbeat_timeout_s=overrides.get("heartbeat_timeout_s", 10.0),
        dead_timeout_s=overrides.get("dead_timeout_s", 30.0),
        max_restarts=overrides.get("max_restarts", 3),
        pel_alert_threshold=overrides.get("pel_alert_threshold", 100),
    )
    return wd


# ---- heartbeat tracking --------------------------------------------------

async def test_heartbeat_marks_module_alive():
    bus = InMemoryBus()
    wd = await _new_watchdog(bus)
    await wd.handle_heartbeat(_heartbeat_envelope("risk_gate", pid=999), "rid")

    entry = wd.registry["risk_gate"]
    assert entry["status"] == "alive"
    assert entry["pid"] == 999
    assert entry["last_seen"] is not None


async def test_module_marked_degraded_when_heartbeat_missing():
    bus = InMemoryBus()
    wd = await _new_watchdog(bus, heartbeat_timeout_s=0.1, dead_timeout_s=5.0)

    degraded: list[dict] = []
    async def cap(env, _mid): degraded.append(env)
    await bus.subscribe("health.degraded", "spy", "s", cap)

    await wd.handle_heartbeat(_heartbeat_envelope("risk_gate"), "rid")
    # Force-age last_seen.
    wd.registry["risk_gate"]["last_seen"] = time.time() - 1.0
    # Stub the restart so we don't spawn subprocesses.
    async def _noop_restart(_name): return None
    with patch.object(wd, "_attempt_restart", new=_noop_restart):
        await wd._check_once()
    await asyncio.sleep(0.05)

    assert any(e["data"]["owner_id"] == "risk_gate" for e in degraded)
    assert wd.registry["risk_gate"]["status"] in ("degraded", "dead")


async def test_module_marked_dead_after_dead_timeout():
    bus = InMemoryBus()
    wd = await _new_watchdog(bus, heartbeat_timeout_s=0.1, dead_timeout_s=0.2)
    deads: list[dict] = []
    async def cap(env, _mid): deads.append(env)
    await bus.subscribe("health.dead", "spy", "s", cap)

    await wd.handle_heartbeat(_heartbeat_envelope("risk_gate"), "rid")
    wd.registry["risk_gate"]["last_seen"] = time.time() - 1.0
    wd.registry["risk_gate"]["restart_attempts"] = 99  # cap exceeded
    async def _noop_restart(_name): return None
    with patch.object(wd, "_attempt_restart", new=_noop_restart):
        await wd._check_once()
    await asyncio.sleep(0.05)

    assert any(e["data"]["owner_id"] == "risk_gate" for e in deads)
    assert wd.registry["risk_gate"]["status"] == "dead"


async def test_restart_capped_at_max_restarts():
    bus = InMemoryBus()
    wd = await _new_watchdog(bus, heartbeat_timeout_s=0.1, max_restarts=2)
    restarts: list[dict] = []
    async def cap(env, _mid): restarts.append(env)
    await bus.subscribe("health.restarted", "spy", "s", cap)

    await wd.handle_heartbeat(_heartbeat_envelope("risk_gate"), "rid")

    calls = []
    async def fake_spawn(self, name):
        calls.append(name)
    with patch("modules.watchdog.subprocess.Popen") as popen:
        popen.return_value = MagicMock(pid=4321)
        for _ in range(5):
            wd.registry["risk_gate"]["last_seen"] = time.time() - 1.0
            await wd._check_once()
            await asyncio.sleep(0.01)

    # Restart subprocess should have been called at most max_restarts times.
    assert popen.call_count <= 2


async def test_restored_after_degraded():
    bus = InMemoryBus()
    wd = await _new_watchdog(bus, heartbeat_timeout_s=0.1)
    restored: list[dict] = []
    async def cap(env, _mid): restored.append(env)
    await bus.subscribe("health.restored", "spy", "s", cap)

    await wd.handle_heartbeat(_heartbeat_envelope("risk_gate"), "rid")
    wd.registry["risk_gate"]["status"] = "degraded"
    await wd.handle_heartbeat(_heartbeat_envelope("risk_gate"), "rid2")
    await asyncio.sleep(0.05)

    assert any(e["data"]["owner_id"] == "risk_gate" for e in restored)
    assert wd.registry["risk_gate"]["status"] == "alive"


# ---- PEL monitoring ------------------------------------------------------

async def test_pel_growing_alert_published():
    bus = InMemoryBus()
    wd = await _new_watchdog(bus, pel_alert_threshold=2)

    # Stub the bus.get_pending to report a deep PEL for one module.
    async def fake_pending(topic, group):
        return [{"message_id": str(i)} for i in range(5)]
    bus.get_pending = fake_pending  # type: ignore[method-assign]

    wd.monitored_streams = [("orders.new", "risk_gate")]

    alerts: list[dict] = []
    async def cap(env, _mid): alerts.append(env)
    await bus.subscribe("health.pel_growing", "spy", "s", cap)

    await wd._pel_check_once()
    await asyncio.sleep(0.05)

    assert any(e["data"]["owner_id"] == "risk_gate" for e in alerts)
    assert alerts[-1]["data"]["count"] == 5


# ---- Sentinel failover ---------------------------------------------------

async def test_sentinel_switch_master_publishes_failover():
    bus = InMemoryBus()
    wd = await _new_watchdog(bus)
    failovers: list[dict] = []
    async def cap(env, _mid): failovers.append(env)
    await bus.subscribe("health.bus.failover", "spy", "s", cap)

    msg = {
        "type":    "message",
        "channel": b"+switch-master",
        "data":    b"mymaster 127.0.0.1 6379 127.0.0.1 6380",
    }
    await wd.handle_sentinel_event(msg)
    await asyncio.sleep(0.05)

    assert len(failovers) == 1
    d = failovers[0]["data"]
    assert d["master"] == "mymaster"
    assert d["new_host"] == "127.0.0.1"
    assert d["new_port"] == 6380
