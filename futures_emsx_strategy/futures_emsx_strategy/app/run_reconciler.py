"""reconciler-service: compare internal positions against an external source (broker, clearing)."""
from __future__ import annotations

import json
import signal
import time
from pathlib import Path

import click

from ..core.logging import get_logger
from ..portfolio.reconciliation import Reconciler
from .wiring import build_services

log = get_logger(__name__)


@click.command()
@click.option("--config-dir", default="config")
@click.option("--external-positions", default=None, help="Path to JSON {symbol: qty}")
@click.option("--interval-seconds", default=60, type=int)
def main(config_dir: str, external_positions: str | None, interval_seconds: int) -> None:
    services = build_services(config_dir)
    reconciler = Reconciler(services.positions)

    def load_external() -> dict[str, int]:
        if not external_positions:
            return {}
        p = Path(external_positions)
        if not p.exists():
            return {}
        return {k: int(v) for k, v in json.loads(p.read_text()).items()}

    stop = False

    def _shutdown(*_a) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    log.info("reconciler_started", interval=interval_seconds)
    while not stop:
        breaks = reconciler.compare(load_external())
        if breaks:
            services.alerts.alert(
                "warning",
                "position_breaks_detected",
                count=len(breaks),
                details=[
                    {"instrument": b.instrument, "internal": b.internal_qty, "external": b.external_qty}
                    for b in breaks
                ],
            )
        else:
            log.info("reconcile_ok", positions=services.positions.snapshot())
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
