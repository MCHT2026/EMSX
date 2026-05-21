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
    # Load config first so the provider gets host/port/service from market_data.yaml
    # rather than BloombergMarketDataProvider's hard-coded defaults.
    from ..config.loader import load_app_config
    cfg = load_app_config(config_dir)

    md = None
    if bloomberg:
        from ..market_data.bloomberg_provider import BloombergMarketDataProvider
        # We subscribe under bloomberg_topic but report ticks under symbol so
        # the rest of the system (positions, risk, etc.) uses a stable key.
        topic_to_symbol = {
            i.bloomberg_topic: i.symbol for i in cfg.instruments.instruments
        }
        md = BloombergMarketDataProvider(
            host=cfg.market_data.host,
            port=cfg.market_data.port,
            service=cfg.market_data.service,
            historical_service=cfg.market_data.historical_service,
            topic_to_symbol=topic_to_symbol,
        )

    services = build_services(config_dir, metrics_port=metrics_port, market_data=md)
    # Subscribe under bloomberg_topic so logical symbol != topic is supported.
    topics = [i.bloomberg_topic for i in services.config.instruments.instruments]

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
    services.market_data.subscribe(topics, services.config.market_data.fields)
    services.bus.start()

    stop = False

    def _shutdown(*_a) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    log.info("market_data_service_started", topics=topics)
    while not stop:
        time.sleep(1.0)
    services.market_data.stop()
    services.bus.stop()


if __name__ == "__main__":
    main()
