"""08: Full pipeline with PAPER execution but LIVE Bloomberg market data.

Safety: READ-ONLY (execution-wise). Market data is read-only too.

What this proves:
  - Strategy reacts to real-time bars.
  - OrderManager / risk gate / paper adapter cooperate end-to-end.
  - Positions and PnL move based on simulated fills against live marks.

This is the safest way to shadow-test a strategy against real market
conditions before doing anything in EMSX. Nothing reaches the broker.

Run::

    python -m examples.08_pipeline_paper_live --minutes 30
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
from futures_emsx_strategy.core.logging import get_logger
from futures_emsx_strategy.orders.lifecycle import OrderLifecycle, OrderRecord

log = get_logger(__name__)


@click.command()
@click.option("--config-dir", default=DEFAULT_CONFIG_DIR)
@click.option("--minutes", default=30, type=int, help="0 = until SIGINT")
def main(config_dir: str, minutes: int) -> None:
    setup()
    require_blpapi()
    banner("Live data → strategy → paper fills", "READ-ONLY")

    cfg = load_config(config_dir)
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
    services = build_services(config_dir, metrics_port=None, market_data=md)
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
        services.event_log.append("BarClosed", bar)
        services.bus.publish(BARS, bar)
        for strat in by_instrument.get(bar.instrument, []):
            for tgt in strat.on_bar(bar, services.positions):
                services.event_log.append("TargetPosition", tgt)
                services.bus.publish(TARGETS, tgt)

    def on_target(_topic: str, target: TargetPosition) -> None:
        for intent in services.order_manager.on_target(target):
            services.bus.publish(INTENTS, intent)
            decision = services.risk.validate(intent).decision
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
                f"SUBMIT (paper)  {intent.instrument}  {intent.side.value} {intent.qty}  "
                f"reason={intent.metadata.get('reason')}"
            )
            ack = services.execution.submit_order(intent)
            lifecycle.set_venue_info(iid, ack.order_id, ack.route_id)

    def on_update(update: ExecutionUpdate) -> None:
        rec = lifecycle.resolve(update.order_id)
        if rec is not None:
            services.working.upsert(
                rec.order_id, update.instrument, rec.side, update.leaves_qty, update.status
            )
            lifecycle.update_status(rec.order_id, update.status, filled_qty=update.filled_qty)

    def on_fill(fill: FillUpdate) -> None:
        services.positions.apply_fill(fill)
        services.pnl.apply_fill(fill)
        services.fills.record(fill)
        services.fill_repo.insert(fill)
        avg = services.positions.avg_cost(fill.instrument) or 0.0
        services.position_repo.upsert(
            fill.instrument, services.positions.position(fill.instrument), avg
        )
        print(
            f"FILL    {fill.instrument}  {fill.side.value} {fill.fill_qty} @ {fill.fill_price:.4f}"
            f"   pos={services.positions.position(fill.instrument)}  "
            f"realized_pnl={services.pnl.realized():.2f}"
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
    print(f"Realized PnL: {services.pnl.realized():.2f}\n")


if __name__ == "__main__":
    main()
