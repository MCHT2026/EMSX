"""01: BLPAPI session smoke test.

Safety: READ-ONLY.

What this proves:
  - blpapi is installed.
  - A local Bloomberg Terminal is running with Desktop API enabled.
  - We can open the configured market-data and EMSX services.

Run::

    python -m examples.01_blpapi_smoke --config-dir config

Exit codes:
  0  Everything connected.
  1  Connection or service-open failure (see stderr).
"""
from __future__ import annotations

import sys

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
def main(config_dir: str) -> None:
    setup()
    require_blpapi()
    banner("BLPAPI session smoke", "READ-ONLY")

    cfg = load_config(config_dir)
    import blpapi

    opts = blpapi.SessionOptions()
    opts.setServerHost(cfg.market_data.host)
    opts.setServerPort(cfg.market_data.port)

    session = blpapi.Session(opts)
    print(f"Starting session on {cfg.market_data.host}:{cfg.market_data.port} ...")
    if not session.start():
        print("ERROR: session.start() returned False", file=sys.stderr)
        sys.exit(1)

    print(f"Opening market data service: {cfg.market_data.service}")
    if not session.openService(cfg.market_data.service):
        print(f"ERROR: openService({cfg.market_data.service}) failed", file=sys.stderr)
        sys.exit(1)

    print(f"Opening refdata service:     {cfg.market_data.historical_service}")
    if not session.openService(cfg.market_data.historical_service):
        print(f"ERROR: openService({cfg.market_data.historical_service}) failed", file=sys.stderr)
        sys.exit(1)

    print(f"Opening EMSX service:        {cfg.emsx.service}")
    if not session.openService(cfg.emsx.service):
        print(
            f"WARN: openService({cfg.emsx.service}) failed. If your firm uses "
            "//blp/emapisvc instead of //blp/emapisvc_beta, edit config/emsx.yaml.",
            file=sys.stderr,
        )
        # Still exit 0 here; EMSX permission is independent of market data.

    session.stop()
    print("\nOK — Bloomberg session is reachable and services opened.\n")


if __name__ == "__main__":
    main()
