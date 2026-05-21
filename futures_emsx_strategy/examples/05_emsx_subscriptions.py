"""05: EMSX read-only subscriptions.

Safety: READ-ONLY.

What this proves:
  - You can open and read from the EMSX order and route subscription topics.
  - The mapper translates the wire fields (EMSX_STATUS, EMSX_FILLED, ...) into
    our internal ExecutionUpdate / FillUpdate events.

This will show every order and route currently live in your EMSX session,
plus any new ones placed manually in the Terminal while the script is up.

Run::

    python -m examples.05_emsx_subscriptions --seconds 60
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
from futures_emsx_strategy.core.events import ExecutionUpdate, FillUpdate


@click.command()
@click.option("--config-dir", default=DEFAULT_CONFIG_DIR)
@click.option("--seconds", default=60, type=int, help="0 = until SIGINT")
def main(config_dir: str, seconds: int) -> None:
    setup()
    require_blpapi()
    banner("EMSX subscriptions (order + route)", "READ-ONLY")

    cfg = load_config(config_dir)

    from futures_emsx_strategy.execution.emsx_mapper import EMSXMapper
    from futures_emsx_strategy.execution.emsx_subscriptions import EMSXSubscriptions

    mapper = EMSXMapper(cfg.emsx)
    subs = EMSXSubscriptions(host=cfg.emsx.host, port=cfg.emsx.port)

    def on_route(msg: dict) -> None:
        update: ExecutionUpdate = mapper.from_route_subscription(msg)
        print(
            f"ROUTE  seq={msg.get('EMSX_SEQUENCE')} route={msg.get('EMSX_ROUTE_ID')} "
            f"{update.instrument:<20} status={update.status.value} "
            f"filled={update.filled_qty} leaves={update.leaves_qty}"
        )
        if msg.get("EMSX_FILL_AMOUNT") and msg.get("EMSX_FILL_PRICE"):
            fill: FillUpdate = mapper.from_fill_message(msg)
            if fill.fill_qty > 0:
                print(f"  FILL  qty={fill.fill_qty} px={fill.fill_price:.4f}")

    def on_order(msg: dict) -> None:
        update: ExecutionUpdate = mapper.from_route_subscription(msg)
        print(
            f"ORDER  seq={msg.get('EMSX_SEQUENCE')} {update.instrument:<20} "
            f"status={update.status.value}"
        )

    subs.on_route_update(on_route)
    subs.on_order_update(on_order)
    subs.start()
    print("EMSX subscriptions live. Place a manual order in the Terminal to see it appear.\n")

    stop = install_signal_handler()
    start = time.time()
    while not stop.is_set() and (seconds == 0 or time.time() - start < seconds):
        time.sleep(0.5)

    subs.stop()
    print("\nDone.\n")


if __name__ == "__main__":
    main()
