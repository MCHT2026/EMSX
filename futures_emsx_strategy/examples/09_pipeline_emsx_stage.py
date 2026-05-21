"""09: Full pipeline with LIVE Bloomberg data and EMSX STAGE-ONLY execution.

Safety: STAGES-IN-EMSX.

What this does:
  - Real BLPAPI market data drives the strategy.
  - Approved orders go to EMSX via ``CreateOrder`` (no auto-route).
  - The trader sees each order in EMSX with status NEW; nothing is routed.
  - EMSX subscriptions stream status/fills back so the lifecycle in this
    process stays in sync with whatever the trader does in EMSX.

This is the Phase 2 deployment shape recommended in the architecture doc:
real wiring, real EMSX, but the human still presses the route button.

Run::

    python -m examples.09_pipeline_emsx_stage --minutes 30

Tip: stage with tight risk limits so you can validate the produced orders
match what you expect before progressing to Phase 3 (example 10).
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import click

from examples._common import (
    DEFAULT_CONFIG_DIR,
    banner,
    install_signal_handler,
    load_config,
    require_blpapi,
    setup,
)
from futures_emsx_strategy.app.topics import (
    BARS,
    EXECUTION_UPDATES,
    FILLS,
    INTENTS,
    RISK_DECISIONS,
    TARGETS,
    TICKS,
)
from futures_emsx_strategy.app.wiring import build_services
from futures_emsx_strategy.core.enums import OrderStatus
from futures_emsx_strategy.core.events import (
    BarClosed,
    ExecutionUpdate,
    FillUpdate,
    MarketTick,
    TargetPosition,
)
from futures_emsx_strategy.orders.lifecycle import OrderLifecycle, OrderRecord


@click.command()
@click.option("--config-dir", default=DEFAULT_CONFIG_DIR)
@click.option("--minutes", default=30, type=int, help="0 = until SIGINT")
def main(config_dir: str, minutes: int) -> None:
    setup()
    require_blpapi()
    banner("Live data → strategy → EMSX stage-only", "STAGES-IN-EMSX")

    cfg = load_config(config_dir)

    from futures_emsx_strategy.execution.emsx_adapter import EMSXExecutionAdapter
    from futures_emsx_strategy.market_data.bloomberg_provider import (
        BloombergMarketDataProvider,
    )

    topic_to_symbol = {i.bloomberg_topic: i.symbol for i in cfg.instruments.instruments}
    md = BloombergMarketDataProvider(
        host=cfg.market_data.host,
        port=cfg.market_data.port,
        service=cfg.market_data.service,
        historical_service=cfg.market_data.historical_service,
        topic_to_symbol=topic_to_symbol,
    )
    # auto_route=False is the safety: orders are CreateOrder, NOT routed.
    execution = EMSXExecutionAdapter(cfg.emsx, auto_route=False)
    services = build_services(
        config_dir, metrics_port=None, market_data=md, execution=execution
    )
    lifecycle = OrderLifecycle()

    by_instrument: dict[str, list] = {}
    for s in services.strategies:
        by_instrument.setdefault(s.instrument, []).append(s)

    def on_tick(tick: MarketTick) -> None:
        services.tick_store.append(tick)
        services.stale_monitor.on_tick(tick)
        services.bar_builder.on_tick(tick)
        services.bus.publish(TICKS, tick)

    def on_bar(bar: BarClosed) -> None:
        services.bus.publish(BARS, bar)
        for strat in by_instrument.get(bar.instrument, []):
            for tgt in strat.on_bar(bar, services.positions):
                services.bus.publish(TARGETS, tgt)

    def on_target(_topic: str, target: TargetPosition) -> None:
        for intent in services.order_manager.on_target(target):
            services.bus.publish(INTENTS, intent)
            decision = services.risk.validate(intent).decision
            services.risk_decision_repo.insert(decision)
            services.bus.publish(RISK_DECISIONS, decision)
            if not decision.approved:
                print(f"RISK REJECT  {intent.instrument} reasons={decision.reasons}")
                continue
            if not services.idempotency.claim(intent.idempotency_key):
                continue

            iid = intent.idempotency_key
            services.working.upsert(
                iid, intent.instrument, intent.side, intent.qty, OrderStatus.NEW
            )
            lifecycle.register(
                OrderRecord(
                    order_id=iid,
                    strategy_id=intent.strategy_id,
                    instrument=intent.instrument,
                    side=intent.side,
                    qty=intent.qty,
                    idempotency_key=intent.idempotency_key,
                    created_at=datetime.now(timezone.utc),
                    status=OrderStatus.NEW,
                )
            )
            print(
                f"STAGE → EMSX  {intent.instrument}  {intent.side.value} {intent.qty}  "
                f"reason={intent.metadata.get('reason')}"
            )
            ack = services.execution.submit_order(intent)
            lifecycle.set_venue_info(iid, ack.order_id, ack.route_id)
            print(
                f"  EMSX_SEQUENCE={ack.order_id}  accepted={ack.accepted}  "
                f"message={ack.message}"
            )

    def on_update(update: ExecutionUpdate) -> None:
        rec = lifecycle.resolve(update.order_id)
        if rec is not None:
            services.working.upsert(
                rec.order_id, update.instrument, rec.side, update.leaves_qty, update.status
            )
            lifecycle.update_status(rec.order_id, update.status, filled_qty=update.filled_qty)
            print(
                f"  EMSX update  seq={rec.venue_order_id} status={update.status.value} "
                f"filled={update.filled_qty} leaves={update.leaves_qty}"
            )
        services.bus.publish(EXECUTION_UPDATES, update)

    def on_fill(fill: FillUpdate) -> None:
        services.positions.apply_fill(fill)
        services.pnl.apply_fill(fill)
        services.fills.record(fill)
        services.fill_repo.insert(fill)
        avg = services.positions.avg_cost(fill.instrument) or 0.0
        services.position_repo.upsert(
            fill.instrument, services.positions.position(fill.instrument), avg
        )
        services.bus.publish(FILLS, fill)
        print(
            f"FILL    {fill.instrument}  {fill.side.value} {fill.fill_qty} @ {fill.fill_price:.4f}"
        )

    md.on_tick(on_tick)
    services.bar_builder.on_bar(on_bar)
    services.execution.on_execution_update(on_update)
    services.execution.on_fill(on_fill)
    services.bus.subscribe(TARGETS, on_target)
    services.execution.start()
    md.start()
    md.subscribe(
        [i.bloomberg_topic for i in services.config.instruments.instruments],
        services.config.market_data.fields,
    )
    services.bus.start()

    print(
        "\nLIVE — orders will be STAGED in EMSX (NOT routed). Open the EMSX "
        "panel in the Terminal to see them appear. Ctrl+C to stop.\n"
    )

    stop = install_signal_handler()
    start = time.time()
    while not stop.is_set() and (minutes == 0 or time.time() - start < minutes * 60):
        time.sleep(0.5)

    services.bar_builder.flush()
    services.execution.stop()
    md.stop()
    services.bus.stop()
    services.db.close()
    print("\nFinal positions:", services.positions.snapshot())


if __name__ == "__main__":
    main()
