"""Watchdog — health monitor and restart manager.

Spec mapping (see oms_emsx_spec.md §Watchdog):

- Subscribes to ``health.heartbeat`` and ``health.*``.
- Publishes ``health.degraded``, ``health.restored``, ``health.restarted``,
  ``health.dead``, ``health.pel_growing``, ``health.bus.failover``.
- Tracks a registry of modules with ``last_seen``, ``status``, ``pid``,
  ``restart_attempts``.
- Check loop every ``WATCHDOG_CHECK_INTERVAL_S`` seconds:
  - last_seen > HEARTBEAT_TIMEOUT_S    -> degraded + attempt restart
  - last_seen > WATCHDOG_DEAD_TIMEOUT_S -> dead
  - max 3 restart attempts (WATCHDOG_MAX_RESTARTS)
- PEL check loop every 30s: any (topic, group) with PEL > threshold gets a
  ``health.pel_growing`` alert.
- Subscribes to the Sentinel ``+switch-master`` pub/sub channel to publish
  ``health.bus.failover``.
"""
from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from config import settings
from core.base_module import BaseModule
from core.event_bus import EventBus

log = logging.getLogger(__name__)


class Watchdog(BaseModule):

    def __init__(
        self,
        bus: EventBus,
        known_modules: dict[str, str] | None = None,
        heartbeat_timeout_s: float | None = None,
        dead_timeout_s: float | None = None,
        max_restarts: int | None = None,
        pel_alert_threshold: int | None = None,
        check_interval_s: float | None = None,
        pel_check_interval_s: float | None = None,
        monitored_streams: list[tuple[str, str]] | None = None,
    ) -> None:
        super().__init__(name="watchdog", bus=bus)
        self.known_modules        = dict(known_modules or settings.KNOWN_MODULES)
        self.heartbeat_timeout_s  = heartbeat_timeout_s  if heartbeat_timeout_s  is not None else settings.HEARTBEAT_TIMEOUT_S
        self.dead_timeout_s       = dead_timeout_s       if dead_timeout_s       is not None else settings.WATCHDOG_DEAD_TIMEOUT_S
        self.max_restarts         = max_restarts         if max_restarts         is not None else settings.WATCHDOG_MAX_RESTARTS
        self.pel_alert_threshold  = pel_alert_threshold  if pel_alert_threshold  is not None else settings.PEL_ALERT_THRESHOLD
        self.check_interval_s     = check_interval_s     if check_interval_s     is not None else settings.WATCHDOG_CHECK_INTERVAL_S
        self.pel_check_interval_s = pel_check_interval_s if pel_check_interval_s is not None else settings.WATCHDOG_PEL_CHECK_INTERVAL_S
        # Stream/group pairs to monitor for PEL depth.
        self.monitored_streams: list[tuple[str, str]] = monitored_streams or [
            ("orders.new",      "risk_gate"),
            ("orders.approved", "emsx_gateway"),
        ]

        # Pre-seed registry with known modules; populated as heartbeats arrive.
        self.registry: dict[str, dict[str, Any]] = {
            name: {"last_seen": None, "status": "unknown",
                   "pid": None, "restart_attempts": 0}
            for name in self.known_modules
        }

    # ---- heartbeat ingestion --------------------------------------------

    async def handle_heartbeat(self, envelope: dict, _msg_id: str) -> None:
        owner_id = envelope.get("owner_id")
        if not owner_id:
            return
        pid = envelope.get("data", {}).get("pid")
        entry = self.registry.setdefault(owner_id, {
            "last_seen": None, "status": "unknown",
            "pid": None, "restart_attempts": 0,
        })
        was_degraded = entry["status"] in ("degraded", "dead")
        entry["last_seen"] = time.time()
        entry["pid"]       = pid
        entry["status"]    = "alive"
        if was_degraded:
            await self.publish("health.restored",
                               {"owner_id": owner_id, "pid": pid})
            # Reset restart counter on recovery so the budget resets.
            entry["restart_attempts"] = 0

    # ---- health.* passthrough -------------------------------------------

    async def handle_health(self, envelope: dict, _msg_id: str) -> None:
        """Catch-all for non-heartbeat health.* events; informational only.

        The bus catch-all subscription delivers heartbeats *and* every other
        health.* event, so we filter heartbeats here (already handled).
        """
        if envelope.get("topic") == "health.heartbeat":
            return
        # Currently no aggregated derivation; subclasses or future
        # operators may add logic here. Keep as a hook.
        log.debug("watchdog: %s", envelope.get("topic"))

    # ---- check loop -----------------------------------------------------

    async def _check_once(self) -> None:
        now = time.time()
        for name, entry in self.registry.items():
            last = entry["last_seen"]
            if last is None:
                continue
            age = now - last
            if age > self.dead_timeout_s:
                if entry["status"] != "dead":
                    entry["status"] = "dead"
                    await self.publish("health.dead",
                                       {"owner_id": name,
                                        "last_seen_age_s": age,
                                        "pid": entry["pid"]})
            elif age > self.heartbeat_timeout_s:
                if entry["status"] != "degraded":
                    entry["status"] = "degraded"
                    await self.publish("health.degraded",
                                       {"owner_id": name,
                                        "last_seen_age_s": age,
                                        "pid": entry["pid"]})
                if entry["restart_attempts"] < self.max_restarts:
                    await self._attempt_restart(name)

    async def _attempt_restart(self, name: str) -> None:
        script = self.known_modules.get(name)
        if script is None:
            log.warning("watchdog: no entry-point known for %s; cannot restart", name)
            return
        entry = self.registry[name]
        entry["restart_attempts"] += 1
        try:
            proc = subprocess.Popen([sys.executable, script])
            entry["pid"] = proc.pid
            await self.publish("health.restarted",
                               {"owner_id": name, "pid": proc.pid,
                                "attempt": entry["restart_attempts"]})
        except Exception as e:
            log.exception("watchdog: failed to restart %s", name)
            await self.publish("health.restart_failed",
                               {"owner_id": name, "error": str(e),
                                "attempt": entry["restart_attempts"]})

    async def _check_loop(self) -> None:  # pragma: no cover
        try:
            while True:
                try:
                    await self._check_once()
                except Exception:
                    log.exception("watchdog _check_once failed")
                await asyncio.sleep(self.check_interval_s)
        except asyncio.CancelledError:
            pass

    # ---- PEL monitor ----------------------------------------------------

    async def _pel_check_once(self) -> None:
        for topic, group in self.monitored_streams:
            try:
                pending = await self.bus.get_pending(topic, group)
            except Exception:
                continue
            count = len(pending) if pending else 0
            if count > self.pel_alert_threshold:
                await self.publish("health.pel_growing",
                                   {"owner_id": group, "topic": topic,
                                    "count": count})

    async def _pel_loop(self) -> None:  # pragma: no cover
        try:
            while True:
                try:
                    await self._pel_check_once()
                except Exception:
                    log.exception("watchdog _pel_check_once failed")
                await asyncio.sleep(self.pel_check_interval_s)
        except asyncio.CancelledError:
            pass

    # ---- Sentinel failover ----------------------------------------------

    async def handle_sentinel_event(self, msg: dict) -> None:
        """Parse a +switch-master pub/sub message and emit health.bus.failover.

        Sentinel format: ``<master_name> <old_host> <old_port> <new_host> <new_port>``
        """
        raw = msg.get("data")
        if isinstance(raw, bytes):
            raw = raw.decode()
        if not raw:
            return
        parts = str(raw).split()
        if len(parts) < 5:
            return
        master, old_host, old_port, new_host, new_port = parts[:5]
        await self.publish("health.bus.failover", {
            "master":   master,
            "old_host": old_host,
            "old_port": int(old_port),
            "new_host": new_host,
            "new_port": int(new_port),
        })

    async def _sentinel_watch(self) -> None:  # pragma: no cover
        watch = getattr(self.bus, "watch_sentinel_switches", None)
        if watch is None:
            return
        try:
            await watch(self.handle_sentinel_event)
        except Exception:
            log.exception("watchdog sentinel watcher exited")

    # ---- run ------------------------------------------------------------

    async def run(self) -> None:  # pragma: no cover
        await self.subscribe("health.heartbeat", self.handle_heartbeat)
        await self.subscribe("health.*",         self.handle_health)
        await self.on_start()
        tasks = [
            asyncio.create_task(self._check_loop(),    name="wd:check"),
            asyncio.create_task(self._pel_loop(),      name="wd:pel"),
            asyncio.create_task(self._sentinel_watch(),name="wd:sentinel"),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await self.on_stop()


def main() -> None:  # pragma: no cover
    import logging as _logging
    from core.redis_bus import RedisBus
    _logging.basicConfig(level=_logging.INFO)

    async def _entry() -> None:
        bus = RedisBus()
        await bus.connect()
        try:
            await Watchdog(bus=bus).run()
        finally:
            await bus.disconnect()

    asyncio.run(_entry())


if __name__ == "__main__":  # pragma: no cover
    main()
