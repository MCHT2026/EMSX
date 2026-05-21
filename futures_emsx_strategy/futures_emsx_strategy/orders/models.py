"""Working order book — net working qty by instrument."""
from __future__ import annotations

from threading import Lock

from ..core.enums import OrderStatus, Side, TERMINAL_ORDER_STATUSES


class WorkingOrderBook:
    """Tracks open orders so OrderManager doesn't double-send.

    Holds *signed* working quantity per instrument (buys positive, sells negative).
    """

    def __init__(self) -> None:
        self._by_id: dict[str, tuple[str, Side, int, OrderStatus]] = {}
        self._lock = Lock()

    def upsert(
        self,
        order_id: str,
        instrument: str,
        side: Side,
        leaves_qty: int,
        status: OrderStatus,
    ) -> None:
        with self._lock:
            if status in TERMINAL_ORDER_STATUSES or leaves_qty <= 0:
                self._by_id.pop(order_id, None)
            else:
                self._by_id[order_id] = (instrument, side, leaves_qty, status)

    def remove(self, order_id: str) -> None:
        with self._lock:
            self._by_id.pop(order_id, None)

    def net_working_qty(self, instrument: str) -> int:
        with self._lock:
            total = 0
            for _id, (sym, side, qty, _st) in self._by_id.items():
                if sym == instrument:
                    total += qty * side.sign
            return total

    def snapshot(self) -> list[tuple[str, str, Side, int, OrderStatus]]:
        with self._lock:
            return [(oid, sym, sd, q, st) for oid, (sym, sd, q, st) in self._by_id.items()]
