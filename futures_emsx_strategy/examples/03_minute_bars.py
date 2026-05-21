"""03: Minute bars from live ticks.

Safety: READ-ONLY.

What this proves:
  - MinuteBarBuilder correctly aggregates the live tick stream into OHLCV
    bars and closes each bar deterministically when the next minute arrives.

Run::

    python -m examples.03_minute_bars --seconds 180
    python -m examples.03_minute_bars --instrument "ESM6 Index"
"""
from __future__ import annotations

import time

import click

from examples._common import (
    DEFAULT_CONFIG_DIR,
    banner,
    install_signal_handler,
    load_config,
    require_blpapi,
    setup,
)
from futures_emsx_strategy.core.events import BarClosed, MarketTick
from futures_emsx_strategy.market_data.bar_builder import MinuteBarBuilder


@click.command()
@click.option("--config-dir", default=DEFAULT_CONFIG_DIR)
@click.option("--instrument", default=None)
@click.option("--seconds", default=180, type=int, help="0 = until SIGINT")
@click.option("--interval-minutes", default=1, type=int)
def main(config_dir: str, instrument: str | None, seconds: int, interval_minutes: int) -> None:
    setup()
    require_blpapi()
    banner(f"Live minute bars ({interval_minutes}m)", "READ-ONLY")

    cfg = load_config(config_dir)
    instruments = cfg.instruments.instruments
    if instrument:
        instruments = [i for i in instruments if i.symbol == instrument]
        if not instruments:
            raise SystemExit(f"unknown instrument: {instrument}")

    from futures_emsx_strategy.market_data.bloomberg_provider import (
        BloombergMarketDataProvider,
    )

    topic_to_symbol = {i.bloomberg_topic: i.symbol for i in instruments}
    provider = BloombergMarketDataProvider(
        host=cfg.market_data.host,
        port=cfg.market_data.port,
        service=cfg.market_data.service,
        historical_service=cfg.market_data.historical_service,
        topic_to_symbol=topic_to_symbol,
    )
    bar_builder = MinuteBarBuilder(interval_minutes=interval_minutes)

    def on_bar(bar: BarClosed) -> None:
        print(
            f"BAR  {bar.instrument:<20} {bar.start_time.isoformat()}  "
            f"O={bar.open:.4f} H={bar.high:.4f} L={bar.low:.4f} "
            f"C={bar.close:.4f} V={bar.volume}"
        )

    def on_tick(tick: MarketTick) -> None:
        bar_builder.on_tick(tick)

    bar_builder.on_bar(on_bar)
    provider.on_tick(on_tick)
    provider.start()
    provider.subscribe(
        [i.bloomberg_topic for i in instruments],
        cfg.market_data.fields,
    )
    print(f"Building {interval_minutes}m bars for {[i.symbol for i in instruments]}.")
    print("Bars print as they close. Ctrl+C to stop.\n")

    stop = install_signal_handler()
    start = time.time()
    while not stop.is_set() and (seconds == 0 or time.time() - start < seconds):
        time.sleep(0.5)

    bar_builder.flush()  # emit any open bars on shutdown
    provider.stop()
    print("\nDone.\n")


if __name__ == "__main__":
    main()
