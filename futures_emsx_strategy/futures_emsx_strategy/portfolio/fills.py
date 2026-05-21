"""Append-only ledger of fills, indexed by (order_id, route_id)."""
from __future__ import annotations

from collections import defaultdict
from threading import Lock

from ..core.events import FillUpdate


class FillLedger:
    def __init__(self) -> None:
        self._fills: list[FillUpdate] = []
        self._by_order: dict[str, list[FillUpdate]] = defaultdict(list)
        self._lock = Lock()

    def record(self, fill: FillUpdate) -> None:
        with self._lock:
            self._fills.append(fill)
            self._by_order[fill.order_id].append(fill)

    def for_order(self, order_id: str) -> list[FillUpdate]:
        with self._lock:
            return list(self._by_order.get(order_id, []))

    def all(self) -> list[FillUpdate]:
        with self._lock:
            return list(self._fills)

    def total_filled(self, order_id: str) -> int:
        return sum(f.fill_qty for f in self.for_order(order_id))
