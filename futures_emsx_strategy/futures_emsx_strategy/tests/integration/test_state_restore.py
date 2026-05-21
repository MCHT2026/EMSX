"""Regression: positions are persisted on fill and restored on restart."""
from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

from futures_emsx_strategy.app.wiring import build_services
from futures_emsx_strategy.core.enums import Side
from futures_emsx_strategy.core.events import FillUpdate


def _copy_config(tmp_path: Path) -> Path:
    src = Path(__file__).resolve().parents[3] / "config"
    dst = tmp_path / "config"
    shutil.copytree(src, dst)
    # Redirect environments.yaml to use a per-test DB and silence the metrics port.
    env_path = dst / "environments.yaml"
    env_text = env_path.read_text()
    db = (tmp_path / "fes.db").as_posix()
    env_text = env_text.replace("sqlite:///fes.db", f"sqlite:///{db}")
    env_text = env_text.replace("metrics_port: 9100", "metrics_port: 0")
    env_path.write_text(env_text)
    return dst


def test_positions_restored_across_restart(tmp_path):
    cfg_dir = _copy_config(tmp_path)

    svc = build_services(str(cfg_dir))
    # Apply a fill via the same path the runner uses.
    fill = FillUpdate(
        order_id="k1", route_id="k1", instrument="ESM6 Index",
        side=Side.BUY, fill_qty=3, fill_price=4500.25,
        timestamp=datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc),
    )
    svc.positions.apply_fill(fill)
    svc.fill_repo.insert(fill)
    svc.position_repo.upsert(
        fill.instrument,
        svc.positions.position(fill.instrument),
        svc.positions.avg_cost(fill.instrument) or 0.0,
    )
    assert svc.positions.position("ESM6 Index") == 3
    svc.db.close()

    # Restart: new Services on the same DB.
    svc2 = build_services(str(cfg_dir))
    assert svc2.positions.position("ESM6 Index") == 3
    assert svc2.positions.avg_cost("ESM6 Index") == pytest.approx(4500.25)
    # Persisted fills survive too.
    fills = svc2.fill_repo.for_order("k1")
    assert len(fills) == 1 and fills[0]["fill_qty"] == 3
    svc2.db.close()


def test_starting_from_empty_db_yields_flat_positions(tmp_path):
    cfg_dir = _copy_config(tmp_path)
    svc = build_services(str(cfg_dir))
    assert svc.positions.snapshot() == {}
    svc.db.close()
