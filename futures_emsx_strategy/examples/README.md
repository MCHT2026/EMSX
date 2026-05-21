# Examples — running against a local Bloomberg + EMSX

Progressive examples that exercise the codebase against a real Bloomberg
Terminal and EMSX instance. Run them in order; each one is a smaller surface
than the next so failures are easy to localize.

## Prerequisites

1. Bloomberg Terminal logged in on this machine.
2. EMSX permissioned on your UUID (talk to your Bloomberg rep if not).
3. BLPAPI Python library installed:
   ```bash
   pip install -e ".[bloomberg,dev]"
   ```
4. Edit [config/instruments.yaml](../config/instruments.yaml) — replace `ESM6 Index`
   with an instrument your firm permits you to trade (e.g. the current
   front-month CME ES contract).
5. Edit [config/emsx.yaml](../config/emsx.yaml) — set `service` to whatever your
   firm uses (`//blp/emapisvc` for production, `//blp/emapisvc_beta` for the
   test environment), and fill in `trader_uuid`, `account`, `broker`.

## Safety ladder

Each example is annotated with a **safety level** in its docstring:

| Level                 | What it does                                       | Risk                |
|-----------------------|----------------------------------------------------|---------------------|
| `READ-ONLY`           | BLPAPI session, subscriptions, historical reads    | None — pure reads   |
| `STAGES-IN-EMSX`      | `CreateOrder` only — no route. Trader sees it.     | Wastes EMSX ticket  |
| `MODIFIES-EMSX`       | Cancel / modify of an existing route               | Cancels real orders |
| `AUTO-ROUTES`         | `CreateOrderAndRouteEx` — sends to broker          | **Real money**      |

Always start at the bottom of the ladder. The `AUTO-ROUTES` examples require
an explicit `--i-understand-this-trades-real-money` flag.

## Examples

| # | Script                              | Safety            | What it shows |
|---|-------------------------------------|-------------------|---------------|
| 01 | `01_blpapi_smoke.py`               | READ-ONLY         | BLPAPI session up, service open |
| 02 | `02_live_ticks.py`                 | READ-ONLY         | `BloombergMarketDataProvider` streaming ticks |
| 03 | `03_minute_bars.py`                | READ-ONLY         | `MinuteBarBuilder` consuming the live stream |
| 04 | `04_historical_backfill.py`        | READ-ONLY         | Intraday bar request via `request_historical_bars` |
| 05 | `05_emsx_subscriptions.py`         | READ-ONLY         | EMSX order + route subscription dump |
| 06 | `06_emsx_stage_order.py`           | STAGES-IN-EMSX    | `CreateOrder` (no auto-route); appears in EMSX |
| 07 | `07_emsx_cancel_route.py`          | MODIFIES-EMSX     | `CancelRouteEx` for a specific route |
| 08 | `08_pipeline_paper_live.py`        | READ-ONLY (execution) | Strategy + paper fills, fed by live ticks |
| 09 | `09_pipeline_emsx_stage.py`        | STAGES-IN-EMSX    | Strategy + EMSX stage-only execution (Phase 2) |
| 10 | `10_pipeline_emsx_auto_route.py`   | AUTO-ROUTES       | Strategy + 1-lot auto-route + tight risk (Phase 3) |

## Running

All examples accept `--config-dir` (default `config/`). Most take an
`--instrument` override too. See `--help`.

```bash
# From repo root
python -m examples.01_blpapi_smoke --config-dir config
python -m examples.02_live_ticks   --config-dir config --seconds 30
```

## Stopping cleanly

All examples handle `Ctrl+C` (SIGINT) by stopping the session, flushing the
bar builder, and cleaning up subscriptions.

## Debugging connectivity

If `01_blpapi_smoke` hangs at `Starting session…`:
- Check the Bloomberg Terminal is logged in.
- Check Desktop API is enabled (`API → API Settings → Enable Desktop API`).
- Check the port (`8194` is the BLPAPI Desktop default).
- Try the alternate service name: some firms expose `//blp/emapisvc`
  rather than `//blp/emapisvc_beta`.
