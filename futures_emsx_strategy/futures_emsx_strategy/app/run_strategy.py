"""strategy-service: consumes bars, produces target positions."""
from __future__ import annotations

import signal
import time

import click

from ..core.events import MarketTick
from ..core.logging import get_logger
from .topics import BARS, TARGETS, TICKS
from .wiring import build_services

log = get_logger(__name__)


@click.command()
@click.option("--config-dir", default="config")
@click.option("--metrics-port", default=None, type=int)
def main(config_dir: str, metrics_port: int | None) -> None:
    services = build_services(config_dir, metrics_port=metrics_port)

    by_instrument: dict[str, list] = {}
    for strat in services.strategies:
        by_instrument.setdefault(strat.instrument, []).append(strat)

    def on_bar(_topic: str, bar) -> None:
        # Serializing buses decode to BarClosed via app.topics registry; in-process
        # buses pass the original object. Either way ``bar.instrument`` is safe.
        for strat in by_instrument.get(bar.instrument, []):
            for tgt in strat.on_bar(bar, services.positions):
                services.metrics.signals_total.labels(strategy=strat.strategy_id).inc()
                services.event_log.append("TargetPosition", tgt)
                services.bus.publish(TARGETS, tgt)

    def on_tick(_topic: str, tick: MarketTick) -> None:
        services.tick_store.append(tick)
        services.stale_monitor.on_tick(tick)

    services.bus.subscribe(TICKS, on_tick)
    services.bus.subscribe(BARS, on_bar)
    services.bus.start()
    stop = False

    def _shutdown(*_a) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    log.info("strategy_service_started", strategies=[s.strategy_id for s in services.strategies])
    while not stop:
        time.sleep(1.0)
    services.bus.stop()


if __name__ == "__main__":
    main()
