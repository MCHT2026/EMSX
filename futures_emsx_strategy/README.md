# futures_emsx_strategy

A minute-bar futures strategy framework: Bloomberg market data in, EMSX execution out, with the strategy decoupled from both via a clean event bus.

## Architecture

```
Bloomberg Market Data (BLPAPI / B-PIPE)
        │
        ▼
   Market Data Service ── ticks/bars ─►  Strategy Service ── target positions ─►  Order Manager
                                                                                    │
                                                                                    ▼
                                                                              Risk Gateway
                                                                                    │
                                                                                    ▼
                                                                            EMSX Execution Adapter
                                                                                    │
                                                                                    ▼
                                                                                  EMSX
```

Strategy never imports execution. Execution never imports strategy. Everything is glued together via the bus.

## Layout

- [futures_emsx_strategy/core/](futures_emsx_strategy/core/) — clock, events, enums, exceptions, identifiers, logging
- [futures_emsx_strategy/config/](futures_emsx_strategy/config/) — typed YAML loader
- [futures_emsx_strategy/market_data/](futures_emsx_strategy/market_data/) — BLPAPI provider, mock provider, minute bar builder, stale-data monitor
- [futures_emsx_strategy/strategy/](futures_emsx_strategy/strategy/) — Strategy interface, MinuteFuturesStrategy (EMA crossover reference)
- [futures_emsx_strategy/portfolio/](futures_emsx_strategy/portfolio/) — positions, fills, PnL, exposure, reconciliation
- [futures_emsx_strategy/orders/](futures_emsx_strategy/orders/) — OrderManager (target -> intent), idempotency, throttling, lifecycle
- [futures_emsx_strategy/risk/](futures_emsx_strategy/risk/) — pre-trade risk gateway, kill switch, session checks
- [futures_emsx_strategy/execution/](futures_emsx_strategy/execution/) — EMSXExecutionAdapter, EMSXRequests, EMSXSubscriptions, EMSXMapper, EMSXStateMachine, PaperAdapter
- [futures_emsx_strategy/storage/](futures_emsx_strategy/storage/) — sqlite event log, state tables, snapshots
- [futures_emsx_strategy/messaging/](futures_emsx_strategy/messaging/) — in-memory / Kafka / Redis-streams buses
- [futures_emsx_strategy/monitoring/](futures_emsx_strategy/monitoring/) — Prometheus metrics, health checks, alerts
- [futures_emsx_strategy/backtest/](futures_emsx_strategy/backtest/) — backtest engine sharing strategy & portfolio code with live
- [futures_emsx_strategy/app/](futures_emsx_strategy/app/) — service entry points
- [config/](config/) — YAML configs (instruments, strategies, risk, EMSX, market data, environments)

## Running locally

```bash
pip install -e .[dev]
python -m futures_emsx_strategy.app.run_all_local --config-dir config --emit-ticks
```

`run_all_local` runs market-data + strategy + execution in one process via the in-memory bus, fed with a synthetic price walk. Use this for paper trading and dev.

## Splitting into services

Same code, different processes, swap `bus: memory` → `bus: kafka` or `redis`:

```bash
python -m futures_emsx_strategy.app.run_market_data --config-dir config --bloomberg
python -m futures_emsx_strategy.app.run_strategy    --config-dir config
python -m futures_emsx_strategy.app.run_execution   --config-dir config --emsx --auto-route
python -m futures_emsx_strategy.app.run_reconciler  --config-dir config --external-positions broker.json
```

## EMSX phased rollout

1. **Paper** — `--paper` (default). Internal paper fills.
2. **EMSX stage-only** — `--emsx --stage-only`. `CreateOrder` only; trader reviews routes in EMSX.
3. **EMSX auto-route, small size** — `--emsx --auto-route` with tight `risk_limits.yaml`.
4. **Production** — Same flags, larger limits, reconciler + kill switch active.

## Tests

```bash
pytest futures_emsx_strategy/tests
```

The `tests/emsx_sim/` suite exercises `EMSXExecutionAdapter` against an in-process fake of BLPAPI — no Bloomberg connection needed.
