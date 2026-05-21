"""07: Cancel a specific EMSX route.

Safety: MODIFIES-EMSX.

What this does:
  - Calls EMSX ``CancelRouteEx`` for the route id you pass in.

Find the route id in EMSX (or in the output of example 05): it has the form
``SEQ:ROUTE``, e.g. ``1234567:1``.

Run::

    python -m examples.07_emsx_cancel_route --route-id 1234567:1
"""
from __future__ import annotations

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
@click.option("--route-id", required=True, help="EMSX route id in SEQ:ROUTE form, e.g. 1234567:1")
def main(config_dir: str, route_id: str) -> None:
    setup()
    require_blpapi()
    banner(f"EMSX CancelRouteEx {route_id}", "MODIFIES-EMSX")

    cfg = load_config(config_dir)

    from futures_emsx_strategy.execution.emsx_adapter import EMSXExecutionAdapter

    adapter = EMSXExecutionAdapter(cfg.emsx, auto_route=False)
    adapter.start()
    ack = adapter.cancel_order(route_id)
    print(f"EMSX ack: accepted={ack.accepted}  message={ack.message}")
    adapter.stop()


if __name__ == "__main__":
    main()
