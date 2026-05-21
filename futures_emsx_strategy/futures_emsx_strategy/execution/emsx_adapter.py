"""High-level EMSX execution adapter.

Composes EMSXRequests (request/response), EMSXSubscriptions (push updates),
EMSXMapper (wire <-> events), and EMSXStateMachine (transition validation).

Order-id contract: the runner pre-registers each order under
``OrderIntent.idempotency_key`` (= the client order id sent to EMSX). EMSX
subscriptions report ``EMSX_SEQUENCE`` (the venue id). To make the runner's
``on_execution_update`` work with a single key, this adapter rewrites every
outgoing ``ExecutionUpdate`` / ``FillUpdate`` to carry the client order id in
``order_id``. The mapping is populated on ``submit_order`` ack. Subscription
events that arrive *before* the ack lands (a real race against the BLPAPI
subscription thread) are buffered keyed by EMSX_SEQUENCE and replayed once
the mapping is known.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone

from ..config.loader import EMSXConfig
from ..core.enums import OrderStatus
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
        # Maps EMSX_SEQUENCE (str) -> client_order_id (intent.idempotency_key).
        self._seq_to_client: dict[str, str] = {}
        # Updates/fills whose EMSX_SEQUENCE has no mapping yet (subscription
        # arrived before submit_order ack landed). Flushed inside submit_order.
        self._pending_updates: dict[str, list[ExecutionUpdate]] = {}
        self._pending_fills: dict[str, list[FillUpdate]] = {}
        self._lock = threading.Lock()

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

        if ack.accepted and ack.order_id:
            client_id = order.idempotency_key
            # Seed the state machine under the client id; venue updates often
            # skip SENT and go straight to WORKING, which is fine from UNKNOWN.
            self.state.seed(client_id, OrderStatus.UNKNOWN)
            with self._lock:
                self._seq_to_client[ack.order_id] = client_id
                pending_u = self._pending_updates.pop(ack.order_id, [])
                pending_f = self._pending_fills.pop(ack.order_id, [])
            # Replay buffered events outside the lock, with the client id swapped in.
            for u in pending_u:
                rewritten = self._rewrite_update(u, client_id)
                self._dispatch_update(rewritten)
            for f in pending_f:
                rewritten_f = self._rewrite_fill(f, client_id)
                self._dispatch_fill(rewritten_f)

        # Returned ack still uses EMSX_SEQUENCE so callers can record both ids
        # (the runner stores ack.order_id as venue_order_id).
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

    # ---- subscription handlers --------------------------------------------

    def _handle_route_update(self, msg: dict) -> None:
        update = self.mapper.from_route_subscription(msg)
        seq = update.order_id  # EMSX_SEQUENCE as str
        with self._lock:
            client_id = self._seq_to_client.get(seq)
            if client_id is None:
                self._pending_updates.setdefault(seq, []).append(update)
                if msg.get("EMSX_FILL_AMOUNT") and msg.get("EMSX_FILL_PRICE"):
                    fill = self.mapper.from_fill_message(msg)
                    if fill.fill_qty > 0:
                        self._pending_fills.setdefault(seq, []).append(fill)
                log.debug("emsx_update_buffered", seq=seq, status=update.status.value)
                return
        rewritten = self._rewrite_update(update, client_id)
        self._dispatch_update(rewritten)
        if msg.get("EMSX_FILL_AMOUNT") and msg.get("EMSX_FILL_PRICE"):
            fill = self.mapper.from_fill_message(msg)
            if fill.fill_qty > 0:
                self._dispatch_fill(self._rewrite_fill(fill, client_id))

    def _handle_order_update(self, msg: dict) -> None:
        update = self.mapper.from_route_subscription(msg)
        seq = update.order_id
        with self._lock:
            client_id = self._seq_to_client.get(seq)
            if client_id is None:
                self._pending_updates.setdefault(seq, []).append(update)
                return
        self._dispatch_update(self._rewrite_update(update, client_id))

    # ---- helpers ----------------------------------------------------------

    @staticmethod
    def _rewrite_update(u: ExecutionUpdate, client_id: str) -> ExecutionUpdate:
        return ExecutionUpdate(
            order_id=client_id,
            route_id=u.route_id,
            instrument=u.instrument,
            status=u.status,
            filled_qty=u.filled_qty,
            avg_price=u.avg_price,
            leaves_qty=u.leaves_qty,
            timestamp=u.timestamp,
            raw=u.raw,
        )

    @staticmethod
    def _rewrite_fill(f: FillUpdate, client_id: str) -> FillUpdate:
        return FillUpdate(
            order_id=client_id,
            route_id=f.route_id,
            instrument=f.instrument,
            side=f.side,
            fill_qty=f.fill_qty,
            fill_price=f.fill_price,
            timestamp=f.timestamp,
        )

    def _dispatch_update(self, u: ExecutionUpdate) -> None:
        self.state.transition(u.order_id, u.status)
        for cb in self._exec_callbacks:
            cb(u)

    def _dispatch_fill(self, f: FillUpdate) -> None:
        for cb in self._fill_callbacks:
            cb(f)
