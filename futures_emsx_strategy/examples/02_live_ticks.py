"""02: Live ticks via BloombergMarketDataProvider.

Safety: READ-ONLY.

What this proves:
  - The provider's BLPAPI subscription thread is delivering MarketTick events.
  - Bloomberg topic -> internal symbol translation works as configured in
    instruments.yaml (the printed instrument is the *symbol*, not the topic).

Run::

    python -m examples.02_live_ticks --seconds 30
    python -m examples.02_live_ticks --instrument "ESM6 Index" --seconds 60

Without --instrument, subscribes to every instrument in instruments.yaml.
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
from futures_emsx_strategy.core.events import MarketTick


@click.command()
@click.option("--config-dir", default=DEFAULT_CONFIG_DIR)
@click.option("--instrument", default=None, help="Restrict to a single instrument symbol")
@click.option("--seconds", default=30, type=int, help="0 = run until SIGINT")
def main(config_dir: str, instrument: str | None, seconds: int) -> None:
    setup()
    require_blpapi()
    banner("Live market data via BLPAPI", "READ-ONLY")

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

    n = [0]

    def on_tick(tick: MarketTick) -> None:
        n[0] += 1
        bid = f"{tick.bid:.4f}" if tick.bid is not None else "-"
        ask = f"{tick.ask:.4f}" if tick.ask is not None else "-"
        last = f"{tick.last:.4f}" if tick.last is not None else "-"
        ts = (tick.exchange_timestamp or tick.receive_timestamp).isoformat()
        print(f"[{n[0]:>5}] {tick.instrument:<20} bid={bid} ask={ask} last={last}  ts={ts}")

    provider.on_tick(on_tick)
    provider.start()
    provider.subscribe(
        [i.bloomberg_topic for i in instruments],
        cfg.market_data.fields,
    )
    print(f"Subscribed to {[i.symbol for i in instruments]}")
    print("Streaming ticks. Ctrl+C to stop.\n")

    stop = install_signal_handler()
    start = time.time()
    while not stop.is_set() and (seconds == 0 or time.time() - start < seconds):
        time.sleep(0.5)

    provider.stop()
    print(f"\nReceived {n[0]} ticks.\n")


if __name__ == "__main__":
    main()
