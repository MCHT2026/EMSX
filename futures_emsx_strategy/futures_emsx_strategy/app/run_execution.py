"""execution-service: target -> intent -> risk -> EMSX. Updates positions/fills from venue subs."""
from __future__ import annotations

import signal
import time
from datetime import datetime, timezone

import click

from ..core.enums import OrderStatus
from ..core.events import ExecutionUpdate, FillUpdate, TargetPosition
from ..core.logging import get_logger
from ..orders.lifecycle import OrderLifecycle, OrderRecord
from .topics import EXECUTION_UPDATES, FILLS, INTENTS, RISK_DECISIONS, TARGETS
from .wiring import build_services

log = get_logger(__name__)


@click.command()
@click.option("--config-dir", default="config")
@click.option("--metrics-port", default=None, type=int)
@click.option("--emsx/--paper", default=False, help="Use real EMSX adapter (requires blpapi)")
@click.option("--auto-route/--stage-only", default=False, help="Phase 2/3: stage-only or auto-route")
def main(config_dir: str, metrics_port: int | None, emsx: bool, auto_route: bool) -> None:
    execution = None
    if emsx:
        from ..config.loader import load_app_config
        from ..execution.emsx_adapter import EMSXExecutionAdapter
        cfg = load_app_config(config_dir)
        execution = EMSXExecutionAdapter(cfg.emsx, auto_route=auto_route)

    services = build_services(config_dir, metrics_port=metrics_port, execution=execution)
    lifecycle = OrderLifecycle()

    def on_target(_topic: str, target: TargetPosition) -> None:
        intents = services.order_manager.on_target(target)
        for intent in intents:
            services.event_log.append("OrderIntent", intent)
            services.bus.publish(INTENTS, intent)

            decision = services.risk.validate(intent).decision
            services.event_log.append("RiskDecision", decision)
            services.bus.publish(RISK_DECISIONS, decision)
            if not decision.approved:
                for reason in decision.reasons:
                    services.metrics.orders_rejected.labels(reason=reason.split(":")[0]).inc()
                continue

            # Internal id = idempotency_key. Stable across paper and EMSX so the
            # synchronous paper callbacks find the record they need to update.
            internal_id = intent.idempotency_key
            services.working.upsert(
                order_id=internal_id,
                instrument=intent.instrument,
                side=intent.side,
                leaves_qty=intent.qty,
                status=OrderStatus.NEW,
            )
            lifecycle.register(
                OrderRecord(
                    order_id=internal_id,
                    strategy_id=intent.strategy_id,
                    instrument=intent.instrument,
                    side=intent.side,
                    qty=intent.qty,
                    idempotency_key=intent.idempotency_key,
                    created_at=datetime.now(timezone.utc),
                    status=OrderStatus.NEW,
                )
            )

            ack = services.execution.submit_order(intent)
            services.event_log.append("ExecutionAck", ack)
            services.metrics.orders_submitted.labels(
                instrument=intent.instrument, side=intent.side.value
            ).inc()
            # Status is owned by callbacks (already advanced by paper adapter
            # synchronously). Only record venue identifiers here.
            lifecycle.set_venue_info(
                order_id=internal_id,
                venue_order_id=ack.order_id,
                route_id=ack.route_id,
            )
            if not ack.accepted:
                lifecycle.update_status(internal_id, OrderStatus.REJECTED)
                services.working.upsert(
                    order_id=internal_id,
                    instrument=intent.instrument,
                    side=intent.side,
                    leaves_qty=0,
                    status=OrderStatus.REJECTED,
                )

    def on_execution_update(update: ExecutionUpdate) -> None:
        services.event_log.append("ExecutionUpdate", update)
        services.bus.publish(EXECUTION_UPDATES, update)
        # update.order_id may be the internal id (paper) or venue id (EMSX).
        rec = lifecycle.resolve(update.order_id)
        if rec is not None:
            lifecycle.update_status(
                order_id=rec.order_id,
                status=update.status,
                filled_qty=update.filled_qty,
                avg_price=update.avg_price,
                route_id=update.route_id,
                timestamp=update.timestamp,
            )
            services.working.upsert(
                order_id=rec.order_id,
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

    services.execution.on_execution_update(on_execution_update)
    services.execution.on_fill(on_fill)
    services.bus.subscribe(TARGETS, on_target)
    services.execution.start()
    services.bus.start()

    stop = False

    def _shutdown(*_a) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    log.info("execution_service_started", emsx=emsx, auto_route=auto_route)
    while not stop:
        time.sleep(1.0)
    services.execution.stop()
    services.bus.stop()


if __name__ == "__main__":
    main()
