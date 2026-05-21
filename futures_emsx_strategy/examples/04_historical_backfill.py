"""04: Historical intraday bar backfill.

Safety: READ-ONLY.

What this proves:
  - ``request_historical_bars`` issues an ``IntradayBarRequest`` against
    ``//blp/refdata`` and yields BarClosed events whose ``end_time`` is
    strictly later than ``start_time`` (the contract for downstream code).

Run::

    python -m examples.04_historical_backfill --instrument "ESM6 Index" --lookback-hours 2
    python -m examples.04_historical_backfill --instrument "ESM6 Index" --interval-minutes 5

Useful for warming up the strategy's indicators before going live.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import click

from examples._common import (
    DEFAULT_CONFIG_DIR,
    banner,
    load_config,
    require_blpapi,
    setup,
)


@click.command()
@click.option("--config-dir", default=DEFAULT_CONFIG_DIR)
@click.option("--instrument", required=True, help="Instrument symbol (must exist in instruments.yaml)")
@click.option("--lookback-hours", default=2, type=int)
@click.option("--interval-minutes", default=1, type=int)
def main(config_dir: str, instrument: str, lookback_hours: int, interval_minutes: int) -> None:
    setup()
    require_blpapi()
    banner(f"Historical {interval_minutes}m bars for {instrument}", "READ-ONLY")

    cfg = load_config(config_dir)
    ins = cfg.instruments.by_symbol(instrument)  # raises ConfigError if unknown

    from futures_emsx_strategy.market_data.bloomberg_provider import (
        BloombergMarketDataProvider,
    )

    provider = BloombergMarketDataProvider(
        host=cfg.market_data.host,
        port=cfg.market_data.port,
        service=cfg.market_data.service,
        historical_service=cfg.market_data.historical_service,
        topic_to_symbol={ins.bloomberg_topic: ins.symbol},
    )
    provider.start()

    end = datetime.now(timezone.utc).replace(microsecond=0)
    start = end - timedelta(hours=lookback_hours)
    print(f"Requesting {interval_minutes}m bars for {ins.bloomberg_topic}")
    print(f"  {start.isoformat()} → {end.isoformat()}\n")

    bars = list(
        provider.request_historical_bars(
            instrument=ins.bloomberg_topic,
            start=start,
            end=end,
            interval_minutes=interval_minutes,
        )
    )
    if not bars:
        print("No bars returned. Possible causes: market closed, instrument typo, ref-data perm.")
    else:
        for b in bars[:10]:
            print(
                f"{b.start_time.isoformat()}  O={b.open:.4f} H={b.high:.4f} "
                f"L={b.low:.4f} C={b.close:.4f} V={b.volume}"
            )
        if len(bars) > 10:
            print(f"... ({len(bars) - 10} more)")
        print(f"\nTotal: {len(bars)} bars.")
        # Contract check
        for b in bars:
            assert b.end_time > b.start_time, "BarClosed.end_time must be > start_time"

    provider.stop()


if __name__ == "__main__":
    main()
