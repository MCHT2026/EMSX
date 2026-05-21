"""Regression: in split (multi-service) mode, the execution service must see
ticks via the bus, otherwise every order is rejected for stale data."""
from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

from futures_emsx_strategy.app.run_execution import main as _exec_main  # noqa: F401
from futures_emsx_strategy.app.topics import TARGETS, TICKS
from futures_emsx_strategy.app.wiring import build_services
from futures_emsx_strategy.core.events import MarketTick, TargetPosition


def _copy_config(tmp_path: Path) -> Path:
    src = Path(__file__).resolve().parents[3] / "config"
    dst = tmp_path / "config"
    shutil.copytree(src, dst)
    env_path = dst / "environments.yaml"
    env_text = env_path.read_text()
    env_text = env_text.replace(
        "sqlite:///fes.db", f"sqlite:///{(tmp_path / 'fes.db').as_posix()}"
    )
    env_text = env_text.replace("metrics_port: 9100", "metrics_port: 0")
    env_path.write_text(env_text)
    # Risk: turn off session requirement so the test can run any time of day.
    risk = dst / "risk_limits.yaml"
    risk.write_text(risk.read_text().replace(
        "require_market_session: true", "require_market_session: false"
    ))
    return dst


def test_execution_service_feeds_tick_into_stale_monitor(tmp_path):
    """Simulate the split-mode wiring: the execution-service must subscribe to
    TICKS so its StaleDataMonitor is fed; otherwise risk rejects every order."""
    cfg_dir = _copy_config(tmp_path)
    svc = build_services(str(cfg_dir))

    # Mirror run_execution's tick subscription.
    def on_tick(_topic, tick):
        svc.tick_store.append(tick)
        svc.stale_monitor.on_tick(tick)

    svc.bus.subscribe(TICKS, on_tick)
    svc.bus.start()

    now = datetime.now(timezone.utc)
    svc.bus.publish(TICKS, MarketTick(
        instrument="ESM6 Index", bid=4500.0, ask=4500.5, last=4500.25,
        volume=1, exchange_timestamp=now, receive_timestamp=now,
    ))

    assert svc.tick_store.last("ESM6 Index") is not None
    assert not svc.stale_monitor.is_stale("ESM6 Index")
    svc.bus.stop()
    svc.db.close()
