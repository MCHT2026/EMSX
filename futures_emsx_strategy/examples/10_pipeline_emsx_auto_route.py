"""10: Full pipeline with LIVE Bloomberg data and EMSX AUTO-ROUTE.

Safety: AUTO-ROUTES.  ===  REAL MONEY  ===

Read this before running:
  - Orders go to the broker via ``CreateOrderAndRouteEx``.
  - Even with the safeties below, this is **real trading**. Anything that
    fills can lose money. You are responsible for verifying that your EMSX
    account is configured the way you intend (broker, account, strategy).
  - The script REFUSES TO RUN unless you pass
    ``--i-understand-this-trades-real-money``.

Built-in safety guards:
  - Hard-caps order qty to 1 unless --max-order-qty is set explicitly.
  - Hard-caps max position to 1 unless --max-position is set explicitly.
  - Kill switch is armed; tripping it stops everything.
  - The script exits when either: --minutes elapses, total fills hit
    --max-fills, or SIGINT is received.

Run::

    python -m examples.10_pipeline_emsx_auto_route \\
        --minutes 5 --max-fills 2 \\
        --i-understand-this-trades-real-money

Recommended progression: only run this AFTER 09 has been used long enough
to confirm the strategy stages orders you would actually accept.
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
from futures_emsx_strategy.risk.limits import RiskLimits


@click.command()
@click.option("--config-dir", default=DEFAULT_CONFIG_DIR)
@click.option("--minutes", default=5, type=int)
@click.option("--max-fills", default=2, type=int, help="Stop after this many fills total")
@click.option("--max-order-qty", default=1, type=int)
@click.option("--max-position", default=1, type=int)
@click.option(
    "--i-understand-this-trades-real-money",
    is_flag=True,
    help="Required to actually start.",
)
def main(
    config_dir: str,
    minutes: int,
    max_fills: int,
    max_order_qty: int,
    max_position: int,
    i_understand_this_trades_real_money: bool,
) -> None:
    setup()
    require_blpapi()
    if not i_understand_this_trades_real_money:
        raise SystemExit(
            "Refusing to start. Re-run with "
            "--i-understand-this-trades-real-money to confirm."
        )

    banner(
        f"AUTO-ROUTE  max_qty={max_order_qty}  max_pos={max_position}  "
        f"stops_after={max_fills}_fills_or_{minutes}m",
        "AUTO-ROUTES",
    )

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
    # auto_route=True is what makes this dangerous.
    execution = EMSXExecutionAdapter(cfg.emsx, auto_route=True)
    services = build_services(
        config_dir, metrics_port=None, market_data=md, execution=execution
    )

    # Override the risk limits with the explicit example-level guards.
    services.risk.limits = RiskLimits(
        max_order_qty=min(max_order_qty, services.risk.limits.max_order_qty),
        max_position=min(max_position, services.risk.limits.max_position),
        max_notional=services.risk.limits.max_notional,
        stale_data_seconds=services.risk.limits.stale_data_seconds,
        require_market_session=services.risk.limits.require_market_session,
        max_orders_per_minute=services.risk.limits.max_orders_per_minute,
        max_cancels_per_minute=services.risk.limits.max_cancels_per_minute,
    )

    lifecycle = OrderLifecycle()
    fill_count = [0]
    stop_for_fills = [False]

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
        if services.kill_switch.is_tripped or stop_for_fills[0]:
            return
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
                f"AUTO-ROUTE → EMSX  {intent.instrument}  {intent.side.value} {intent.qty}"
            )
            ack = services.execution.submit_order(intent)
            lifecycle.set_venue_info(iid, ack.order_id, ack.route_id)
            print(
                f"  EMSX_SEQUENCE={ack.order_id} route={ack.route_id} "
                f"accepted={ack.accepted}"
            )

    def on_update(update: ExecutionUpdate) -> None:
        rec = lifecycle.resolve(update.order_id)
        if rec is not None:
            services.working.upsert(
                rec.order_id, update.instrument, rec.side, update.leaves_qty, update.status
            )
            lifecycle.update_status(rec.order_id, update.status, filled_qty=update.filled_qty)
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
        fill_count[0] += 1
        print(
            f"FILL    {fill.instrument}  {fill.side.value} {fill.fill_qty} @ "
            f"{fill.fill_price:.4f}   total_fills={fill_count[0]}"
        )
        if fill_count[0] >= max_fills:
            print(
                f"\nReached --max-fills ({max_fills}). Tripping kill switch and stopping."
            )
            services.kill_switch.trip("max_fills_reached", actor="example_10")
            stop_for_fills[0] = True

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
    while (
        not stop.is_set()
        and not stop_for_fills[0]
        and (minutes == 0 or time.time() - start < minutes * 60)
    ):
        time.sleep(0.5)

    services.bar_builder.flush()
    services.execution.stop()
    md.stop()
    services.bus.stop()
    services.db.close()
    print("\nFinal positions:", services.positions.snapshot())
    print(f"Total fills: {fill_count[0]}\n")
    print(
        "If any position is open, FLATTEN IT MANUALLY in EMSX before leaving "
        "your desk. This script does not auto-flatten on shutdown."
    )


if __name__ == "__main__":
    main()
