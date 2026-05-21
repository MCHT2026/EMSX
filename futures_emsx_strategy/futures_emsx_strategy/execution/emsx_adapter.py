"""High-level EMSX execution adapter.

Composes EMSXRequests (request/response), EMSXSubscriptions (push updates),
EMSXMapper (wire <-> events), and EMSXStateMachine (transition validation).
"""
from __future__ import annotations

from datetime import datetime, timezone

from ..config.loader import EMSXConfig
from ..core.events import ExecutionAck, ExecutionUpdate, FillUpdate, OrderIntent
from ..core.logging import get_logger
from .base import ExecutionAdapter, ExecutionUpdateCallback, FillCallback
from .emsx_mapper import EMSXMapper
from .emsx_requests import EMSXRequests
from .emsx_state_machine import EMSXStateMachine
from .emsx_subscriptions import EMSXSubscriptions

log = get_logger(__name__)


class EMSXExecutionAdapter(ExecutionAdapter):
    def __init__(
        self,
        config: EMSXConfig,
        auto_route: bool = True,
        requests: EMSXRequests | None = None,
        subscriptions: EMSXSubscriptions | None = None,
    ) -> None:
        self.config = config
        self.auto_route = auto_route
        self.requests = requests or EMSXRequests(config.host, config.port, config.service)
        self.subs = subscriptions or EMSXSubscriptions(config.host, config.port)
        self.mapper = EMSXMapper(config)
        self.state = EMSXStateMachine()
        self._exec_callbacks: list[ExecutionUpdateCallback] = []
        self._fill_callbacks: list[FillCallback] = []

    def start(self) -> None:
        self.requests.start()
        self.subs.start()
        self.subs.on_route_update(self._handle_route_update)
        self.subs.on_order_update(self._handle_order_update)
        log.info("emsx_adapter_started", auto_route=self.auto_route)

    def stop(self) -> None:
        self.subs.stop()
        self.requests.stop()
        log.info("emsx_adapter_stopped")

    def submit_order(self, order: OrderIntent) -> ExecutionAck:
        fields = self.mapper.to_create_order_and_route(order)
        if self.auto_route:
            raw = self.requests.create_order_and_route(fields)
        else:
            raw = self.requests.create_order(fields)
        ack = self.mapper.from_create_response(raw, order)
        log.info(
            "emsx_order_submitted",
            instrument=order.instrument,
            side=order.side.value,
            qty=order.qty,
            order_id=ack.order_id,
            route_id=ack.route_id,
            accepted=ack.accepted,
        )
        return ack

    def cancel_order(self, order_id: str) -> ExecutionAck:
        fields = self.mapper.to_cancel_route(order_id)
        raw = self.requests.cancel_route(fields)
        accepted = raw.get("EMSX_STATUS", "OK") != "ERROR"
        return ExecutionAck(
            order_id=order_id,
            route_id=order_id,
            venue_request_id=str(raw.get("EMSX_REQUEST_SEQ", "")),
            accepted=accepted,
            message=str(raw.get("MESSAGE", "OK")),
            timestamp=datetime.now(timezone.utc),
        )

    def modify_order(self, order_id: str, changes: dict) -> ExecutionAck:
        fields = self.mapper.to_modify_route(order_id, changes)
        raw = self.requests.modify_route(fields)
        accepted = raw.get("EMSX_STATUS", "OK") != "ERROR"
        return ExecutionAck(
            order_id=order_id,
            route_id=order_id,
            venue_request_id=str(raw.get("EMSX_REQUEST_SEQ", "")),
            accepted=accepted,
            message=str(raw.get("MESSAGE", "OK")),
            timestamp=datetime.now(timezone.utc),
        )

    def on_execution_update(self, callback: ExecutionUpdateCallback) -> None:
        self._exec_callbacks.append(callback)

    def on_fill(self, callback: FillCallback) -> None:
        self._fill_callbacks.append(callback)

    def _handle_route_update(self, msg: dict) -> None:
        update = self.mapper.from_route_subscription(msg)
        self.state.transition(update.order_id, update.status)
        for cb in self._exec_callbacks:
            cb(update)
        if msg.get("EMSX_FILL_AMOUNT") and msg.get("EMSX_FILL_PRICE"):
            fill = self.mapper.from_fill_message(msg)
            if fill.fill_qty > 0:
                for cb in self._fill_callbacks:
                    cb(fill)

    def _handle_order_update(self, msg: dict) -> None:
        update = self.mapper.from_route_subscription(msg)
        for cb in self._exec_callbacks:
            cb(update)
