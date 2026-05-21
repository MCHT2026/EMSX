"""Paper-trading adapter. Fills orders instantly against the most recent tick price.

Useful for Phase 1 staging — exercises every layer of the system except the
real EMSX wire. Reads marks from a callable so tests can inject prices.

Order-id convention: the adapter uses ``OrderIntent.idempotency_key`` as the
order_id in the ack and in every subsequent callback. The runner pre-registers
the order under that same id BEFORE calling ``submit_order``, so the
synchronous SENT/FILL/FILLED callbacks find the lifecycle and working-book
entries and update them correctly. Without this convention the runner's
post-submit upsert would race the callbacks and leave phantom working orders.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

from ..core.enums import OrderStatus, OrderType
from ..core.events import ExecutionAck, ExecutionUpdate, FillUpdate, OrderIntent
from ..core.identifiers import new_uuid
from ..core.logging import get_logger
from .base import ExecutionAdapter, ExecutionUpdateCallback, FillCallback

log = get_logger(__name__)


class PaperExecutionAdapter(ExecutionAdapter):
    def __init__(
        self,
        get_mark: Callable[[str], float | None],
        slippage_ticks: float = 0.0,
        tick_size_lookup: Callable[[str], float] | None = None,
    ) -> None:
        self.get_mark = get_mark
        self.slippage_ticks = slippage_ticks
        self.tick_size_lookup = tick_size_lookup or (lambda _s: 0.01)
        self._exec_callbacks: list[ExecutionUpdateCallback] = []
        self._fill_callbacks: list[FillCallback] = []

    def start(self) -> None:
        log.info("paper_adapter_started")

    def stop(self) -> None:
        log.info("paper_adapter_stopped")

    def submit_order(self, order: OrderIntent) -> ExecutionAck:
        order_id = order.idempotency_key
        mark = self.get_mark(order.instrument)
        ack_ts = datetime.now(timezone.utc)
        if mark is None:
            # Push a REJECTED update so any pre-registered working-book entry
            # is cleared rather than left hanging in NEW/SENT forever.
            rejected = ExecutionUpdate(
                order_id=order_id,
                route_id=order_id,
                instrument=order.instrument,
                status=OrderStatus.REJECTED,
                filled_qty=0,
                avg_price=None,
                leaves_qty=0,
                timestamp=ack_ts,
            )
            for cb in self._exec_callbacks:
                cb(rejected)
            return ExecutionAck(
                order_id=order_id,
                route_id=order_id,
                venue_request_id=new_uuid(),
                accepted=False,
                message="no_mark_available",
                timestamp=ack_ts,
            )

        fill_price = self._fill_price(order, mark)

        sent_update = ExecutionUpdate(
            order_id=order_id,
            route_id=order_id,
            instrument=order.instrument,
            status=OrderStatus.SENT,
            filled_qty=0,
            avg_price=None,
            leaves_qty=order.qty,
            timestamp=ack_ts,
        )
        for cb in self._exec_callbacks:
            cb(sent_update)

        fill = FillUpdate(
            order_id=order_id,
            route_id=order_id,
            instrument=order.instrument,
            side=order.side,
            fill_qty=order.qty,
            fill_price=fill_price,
            timestamp=ack_ts,
        )
        for cb in self._fill_callbacks:
            cb(fill)

        filled_update = ExecutionUpdate(
            order_id=order_id,
            route_id=order_id,
            instrument=order.instrument,
            status=OrderStatus.FILLED,
            filled_qty=order.qty,
            avg_price=fill_price,
            leaves_qty=0,
            timestamp=ack_ts,
        )
        for cb in self._exec_callbacks:
            cb(filled_update)

        return ExecutionAck(
            order_id=order_id,
            route_id=order_id,
            venue_request_id=new_uuid(),
            accepted=True,
            message="filled",
            timestamp=ack_ts,
        )

    def cancel_order(self, order_id: str) -> ExecutionAck:
        return ExecutionAck(
            order_id=order_id,
            route_id=order_id,
            venue_request_id=new_uuid(),
            accepted=True,
            message="already_filled",
            timestamp=datetime.now(timezone.utc),
        )

    def modify_order(self, order_id: str, changes: dict) -> ExecutionAck:
        return ExecutionAck(
            order_id=order_id,
            route_id=order_id,
            venue_request_id=new_uuid(),
            accepted=False,
            message="modify_not_supported_on_paper",
            timestamp=datetime.now(timezone.utc),
        )

    def on_execution_update(self, callback: ExecutionUpdateCallback) -> None:
        self._exec_callbacks.append(callback)

    def on_fill(self, callback: FillCallback) -> None:
        self._fill_callbacks.append(callback)

    def _fill_price(self, order: OrderIntent, mark: float) -> float:
        tick = self.tick_size_lookup(order.instrument)
        slip = self.slippage_ticks * tick
        if order.order_type is OrderType.LMT and order.limit_price is not None:
            return order.limit_price
        return mark + slip * order.side.sign
