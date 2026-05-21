"""Validates EMSX status transitions and detects illegal flips.

EMSX is the source of truth, but we still defend against out-of-order
subscription messages. Transitions that are not in the allowed-transitions
table are logged and surfaced via on_violation callbacks.
"""
from __future__ import annotations

from threading import Lock
from typing import Callable

from ..core.enums import OrderStatus, TERMINAL_ORDER_STATUSES
from ..core.logging import get_logger

log = get_logger(__name__)


_ALLOWED: dict[OrderStatus, set[OrderStatus]] = {
    OrderStatus.NEW: {OrderStatus.RISK_APPROVED, OrderStatus.RISK_REJECTED, OrderStatus.UNKNOWN},
    OrderStatus.RISK_APPROVED: {OrderStatus.SENT, OrderStatus.REJECTED, OrderStatus.UNKNOWN},
    OrderStatus.RISK_REJECTED: set(),
    OrderStatus.SENT: {
        OrderStatus.ACCEPTED,
        OrderStatus.WORKING,
        OrderStatus.REJECTED,
        OrderStatus.ROUTE_ERR,
        OrderStatus.UNKNOWN,
    },
    OrderStatus.ACCEPTED: {
        OrderStatus.WORKING,
        OrderStatus.PART_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCEL_PENDING,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.UNKNOWN,
    },
    OrderStatus.WORKING: {
        OrderStatus.PART_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCEL_PENDING,
        OrderStatus.CANCELLED,
        OrderStatus.MODIFY_PENDING,
        OrderStatus.REJECTED,
        OrderStatus.UNKNOWN,
    },
    OrderStatus.PART_FILLED: {
        OrderStatus.PART_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCEL_PENDING,
        OrderStatus.CANCELLED,
        OrderStatus.UNKNOWN,
    },
    OrderStatus.CANCEL_PENDING: {
        OrderStatus.CANCELLED,
        OrderStatus.WORKING,
        OrderStatus.FILLED,
        OrderStatus.PART_FILLED,
        OrderStatus.UNKNOWN,
    },
    OrderStatus.MODIFY_PENDING: {
        OrderStatus.WORKING,
        OrderStatus.PART_FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.UNKNOWN,
    },
    OrderStatus.FILLED: set(),
    OrderStatus.CANCELLED: set(),
    OrderStatus.REJECTED: set(),
    OrderStatus.ROUTE_ERR: {OrderStatus.UNKNOWN},
    OrderStatus.UNKNOWN: set(OrderStatus) - {OrderStatus.NEW},
}


ViolationCallback = Callable[[str, OrderStatus, OrderStatus], None]


class EMSXStateMachine:
    def __init__(self) -> None:
        self._states: dict[str, OrderStatus] = {}
        self._lock = Lock()
        self._violations: list[ViolationCallback] = []

    def current(self, order_id: str) -> OrderStatus:
        with self._lock:
            return self._states.get(order_id, OrderStatus.NEW)

    def on_violation(self, cb: ViolationCallback) -> None:
        self._violations.append(cb)

    def transition(self, order_id: str, new: OrderStatus) -> bool:
        with self._lock:
            current = self._states.get(order_id, OrderStatus.NEW)
            if current in TERMINAL_ORDER_STATUSES and new != current:
                self._raise_violation(order_id, current, new)
                return False
            allowed = _ALLOWED.get(current, set())
            if new not in allowed and new != current:
                self._raise_violation(order_id, current, new)
                return False
            self._states[order_id] = new
            return True

    def _raise_violation(self, order_id: str, current: OrderStatus, new: OrderStatus) -> None:
        log.warning(
            "illegal_state_transition",
            order_id=order_id,
            current=current.value,
            attempted=new.value,
        )
        for cb in self._violations:
            try:
                cb(order_id, current, new)
            except Exception:  # noqa: BLE001
                log.exception("violation_callback_failed")
