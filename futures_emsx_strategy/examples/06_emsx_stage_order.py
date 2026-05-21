"""06: EMSX stage-only order (CreateOrder, no auto-route).

Safety: STAGES-IN-EMSX.

What this does:
  - Calls EMSX ``CreateOrder`` to stage a parent order in your EMSX blotter.
  - **It does NOT call RouteEx**, so nothing leaves EMSX. The trader sees the
    order with status NEW and decides whether to route manually.
  - You can cancel it from EMSX or with example 07.

What this proves:
  - Your UUID is permissioned for EMSX CreateOrder.
  - EMSXMapper.to_create_order_and_route(...) produces the right fields.

Run::

    python -m examples.06_emsx_stage_order \\
        --instrument "ESM6 Index" \\
        --side BUY --qty 1 --order-type LMT --limit-price 4500.00
"""
from __future__ import annotations

from datetime import datetime, timezone

import click

from examples._common import (
    DEFAULT_CONFIG_DIR,
    banner,
    load_config,
    require_blpapi,
    setup,
)
from futures_emsx_strategy.core.enums import OrderType, Side, TimeInForce
from futures_emsx_strategy.core.events import OrderIntent
from futures_emsx_strategy.core.identifiers import idempotency_key


@click.command()
@click.option("--config-dir", default=DEFAULT_CONFIG_DIR)
@click.option("--instrument", required=True)
@click.option("--side", type=click.Choice(["BUY", "SELL"]), required=True)
@click.option("--qty", type=int, required=True)
@click.option("--order-type", type=click.Choice(["MKT", "LMT"]), default="LMT")
@click.option("--limit-price", type=float, default=None)
@click.option("--tif", type=click.Choice(["DAY", "GTC", "IOC", "FOK"]), default="DAY")
def main(
    config_dir: str,
    instrument: str,
    side: str,
    qty: int,
    order_type: str,
    limit_price: float | None,
    tif: str,
) -> None:
    setup()
    require_blpapi()
    banner(f"EMSX CreateOrder (stage-only) {side} {qty} {instrument}", "STAGES-IN-EMSX")

    if order_type == "LMT" and limit_price is None:
        raise SystemExit("--limit-price is required when --order-type LMT")

    cfg = load_config(config_dir)
    cfg.instruments.by_symbol(instrument)  # validates symbol

    from futures_emsx_strategy.execution.emsx_adapter import EMSXExecutionAdapter

    # auto_route=False -> CreateOrder only; nothing leaves EMSX.
    adapter = EMSXExecutionAdapter(cfg.emsx, auto_route=False)
    adapter.start()

    ts = datetime.now(timezone.utc)
    intent = OrderIntent(
        strategy_id="example_06",
        instrument=instrument,
        side=Side(side),
        qty=qty,
        order_type=OrderType(order_type),
        time_in_force=TimeInForce(tif),
        idempotency_key=idempotency_key("example_06", instrument, ts, side, qty),
        source_timestamp=ts,
        limit_price=limit_price,
    )
    print(f"Intent:\n  {intent}\n")

    ack = adapter.submit_order(intent)
    print(f"EMSX ack:")
    print(f"  accepted        = {ack.accepted}")
    print(f"  message         = {ack.message}")
    print(f"  EMSX_SEQUENCE   = {ack.order_id}")
    print(f"  EMSX route_id   = {ack.route_id}")
    print(f"  venue_request_id= {ack.venue_request_id}")
    print(f"  timestamp       = {ack.timestamp.isoformat()}")

    if ack.accepted:
        print(
            "\nThe order is now staged in EMSX. Open the EMSX panel to confirm. "
            "It WILL NOT be routed unless the trader manually routes it from EMSX "
            "(or until you call RouteEx programmatically)."
        )

    adapter.stop()


if __name__ == "__main__":
    main()
