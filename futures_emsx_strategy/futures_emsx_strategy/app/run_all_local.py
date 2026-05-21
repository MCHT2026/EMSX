"""Single-process runner: market-data + strategy + execution in one process via the in-memory bus.

Good for dev, paper trading, and integration tests. The same pieces can be split into
separate processes (see run_market_data, run_strategy, run_execution) when running on Kafka/Redis.
"""
from __future__ import annotations

import signal
import time
from datetime import datetime, timezone

import click

from ..core.enums import OrderStatus
from ..core.events import BarClosed, ExecutionUpdate, FillUpdate, MarketTick, TargetPosition
from ..core.logging import get_logger
from ..market_data.mock_provider import MockMarketDataProvider
from ..orders.lifecycle import OrderLifecycle, OrderRecord
from .topics import BARS, EXECUTION_UPDATES, FILLS, INTENTS, RISK_DECISIONS, TARGETS, TICKS
from .wiring import build_services

log = get_logger(__name__)


@click.command()
@click.option("--config-dir", default="config")
@click.option("--metrics-port", default=None, type=int)
@click.option("--seconds", default=0, type=int, help="0 = run until SIGINT")
@click.option("--emit-ticks/--no-emit-ticks", default=True, help="Drive the mock provider with synthetic ticks")
def main(config_dir: str, metrics_port: int | None, seconds: int, emit_ticks: bool) -> None:
    md = MockMarketDataProvider()
    services = build_services(config_dir, metrics_port=metrics_port, market_data=md)
    lifecycle = OrderLifecycle()

    by_instrument: dict[str, list] = {}
    for strat in services.strategies:
        by_instrument.setdefault(strat.instrument, []).append(strat)

    def on_tick(tick: MarketTick) -> None:
        services.tick_store.append(tick)
        services.stale_monitor.on_tick(tick)
        services.bar_builder.on_tick(tick)
        services.metrics.ticks_total.labels(instrument=tick.instrument).inc()
        services.bus.publish(TICKS, tick)

    def on_bar(bar: BarClosed) -> None:
        services.metrics.bars_total.labels(instrument=bar.instrument).inc()
        services.event_log.append("BarClosed", bar)
        services.bus.publish(BARS, bar)
        for strat in by_instrument.get(bar.instrument, []):
            for tgt in strat.on_bar(bar, services.positions):
                services.metrics.signals_total.labels(strategy=strat.strategy_id).inc()
                services.event_log.append("TargetPosition", tgt)
                services.bus.publish(TARGETS, tgt)

    def on_target(_topic: str, target: TargetPosition) -> None:
        for intent in services.order_manager.on_target(target):
            services.event_log.append("OrderIntent", intent)
            services.bus.publish(INTENTS, intent)
            decision = services.risk.validate(intent).decision
            services.event_log.append("RiskDecision", decision)
            services.bus.publish(RISK_DECISIONS, decision)
            if not decision.approved:
                for reason in decision.reasons:
                    services.metrics.orders_rejected.labels(reason=reason.split(":")[0]).inc()
                continue
            ack = services.execution.submit_order(intent)
            services.event_log.append("ExecutionAck", ack)
            services.metrics.orders_submitted.labels(
                instrument=intent.instrument, side=intent.side.value
            ).inc()
            services.working.upsert(
                order_id=ack.order_id,
                instrument=intent.instrument,
                side=intent.side,
                leaves_qty=intent.qty,
                status=OrderStatus.SENT if ack.accepted else OrderStatus.REJECTED,
            )
            lifecycle.register(
                OrderRecord(
                    order_id=ack.order_id,
                    strategy_id=intent.strategy_id,
                    instrument=intent.instrument,
                    side=intent.side,
                    qty=intent.qty,
                    idempotency_key=intent.idempotency_key,
                    created_at=datetime.now(timezone.utc),
                    status=OrderStatus.SENT if ack.accepted else OrderStatus.REJECTED,
                    venue_order_id=ack.order_id,
                    route_id=ack.route_id,
                )
            )

    def on_execution_update(update: ExecutionUpdate) -> None:
        services.event_log.append("ExecutionUpdate", update)
        services.bus.publish(EXECUTION_UPDATES, update)
        rec = lifecycle.get(update.order_id)
        if rec is not None:
            services.working.upsert(
                order_id=update.order_id,
                instrument=update.instrument,
                side=rec.side,
                leaves_qty=update.leaves_qty,
                status=update.status,
            )
        services.metrics.working_qty.labels(instrument=update.instrument).set(update.leaves_qty)

    def on_fill(fill: FillUpdate) -> None:
        services.event_log.append("FillUpdate", fill)
        services.bus.publish(FILLS, fill)
        services.positions.apply_fill(fill)
        services.pnl.apply_fill(fill)
        services.fills.record(fill)
        services.metrics.fills_total.labels(instrument=fill.instrument).inc()
        services.metrics.open_position.labels(instrument=fill.instrument).set(
            services.positions.position(fill.instrument)
        )

    services.market_data.on_tick(on_tick)
    services.bar_builder.on_bar(on_bar)
    services.execution.on_execution_update(on_execution_update)
    services.execution.on_fill(on_fill)
    services.bus.subscribe(TARGETS, on_target)
    services.market_data.start()
    services.market_data.subscribe(
        [i.symbol for i in services.config.instruments.instruments],
        services.config.market_data.fields,
    )
    services.execution.start()
    services.bus.start()

    stop = [False]

    def _shutdown(*_a) -> None:
        stop[0] = True

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("run_all_local_started")
    start = time.time()
    if emit_ticks:
        _drive_mock_ticks(services, md, stop)
    else:
        while not stop[0] and (seconds == 0 or time.time() - start < seconds):
            time.sleep(0.5)

    services.execution.stop()
    services.market_data.stop()
    services.bus.stop()
    log.info("run_all_local_stopped")


def _drive_mock_ticks(services, md: MockMarketDataProvider, stop: list[bool]) -> None:
    """Simple synthetic price walk for paper/dev runs."""
    import math
    import random

    instruments = [i.symbol for i in services.config.instruments.instruments]
    prices = {sym: 4500.0 + 100 * i for i, sym in enumerate(instruments)}
    t = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    bars_per_minute = 6
    i = 0
    while not stop[0]:
        i += 1
        seconds_offset = (i * (60 // bars_per_minute)) % 60
        ts = t.replace(second=seconds_offset) if seconds_offset < 60 else t
        if seconds_offset == 0 and i > 1:
            t = t.replace(minute=(t.minute + 1) % 60)
            ts = t
        for sym in instruments:
            drift = 0.05 * math.sin(i / 30.0)
            shock = random.gauss(0, 0.5)
            prices[sym] += drift + shock
            md.push_tick(MockMarketDataProvider.tick(sym, prices[sym], ts=ts, volume=1))
        time.sleep(1.0 / bars_per_minute)


if __name__ == "__main__":
    main()
