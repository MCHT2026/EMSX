"""market-data-service: subscribes to Bloomberg, builds bars, publishes ticks+bars."""
from __future__ import annotations

import signal
import time

import click

from ..core.events import BarClosed, MarketTick
from ..core.logging import get_logger
from .topics import BARS, TICKS
from .wiring import build_services

log = get_logger(__name__)


@click.command()
@click.option("--config-dir", default="config", help="Path to config/")
@click.option("--metrics-port", default=None, type=int)
@click.option("--bloomberg/--mock", default=False)
def main(config_dir: str, metrics_port: int | None, bloomberg: bool) -> None:
    md = None
    if bloomberg:
        from ..market_data.bloomberg_provider import BloombergMarketDataProvider
        md = BloombergMarketDataProvider()

    services = build_services(config_dir, metrics_port=metrics_port, market_data=md)
    instruments = [i.symbol for i in services.config.instruments.instruments]

    def on_tick(tick: MarketTick) -> None:
        services.tick_store.append(tick)
        services.stale_monitor.on_tick(tick)
        services.bar_builder.on_tick(tick)
        services.metrics.ticks_total.labels(instrument=tick.instrument).inc()
        services.event_log.append("MarketTick", tick)
        services.bus.publish(TICKS, tick)

    def on_bar(bar: BarClosed) -> None:
        services.metrics.bars_total.labels(instrument=bar.instrument).inc()
        services.event_log.append("BarClosed", bar)
        services.bus.publish(BARS, bar)
        log.info("bar_emitted", instrument=bar.instrument, close=bar.close, end=bar.end_time.isoformat())

    services.market_data.on_tick(on_tick)
    services.bar_builder.on_bar(on_bar)
    services.market_data.start()
    services.market_data.subscribe(instruments, services.config.market_data.fields)
    services.bus.start()

    stop = False

    def _shutdown(*_a) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    log.info("market_data_service_started", instruments=instruments)
    while not stop:
        time.sleep(1.0)
    services.market_data.stop()
    services.bus.stop()


if __name__ == "__main__":
    main()
