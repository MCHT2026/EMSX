"""Lifecycle records: associates an internal order_id with its strategy, key, and EMSX route IDs."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock

from ..core.enums import OrderStatus, Side


@dataclass
class OrderRecord:
    order_id: str
    strategy_id: str
    instrument: str
    side: Side
    qty: int
    idempotency_key: str
    created_at: datetime
    status: OrderStatus = OrderStatus.NEW
    venue_order_id: str | None = None
    route_id: str | None = None
    filled_qty: int = 0
    avg_price: float | None = None
    last_update: datetime | None = None
    reasons: list[str] = field(default_factory=list)


class OrderLifecycle:
    """In-process registry of OrderRecords by order_id."""

    def __init__(self) -> None:
        self._records: dict[str, OrderRecord] = {}
        self._lock = Lock()

    def register(self, rec: OrderRecord) -> None:
        with self._lock:
            self._records[rec.order_id] = rec

    def get(self, order_id: str) -> OrderRecord | None:
        with self._lock:
            return self._records.get(order_id)

    def update_status(
        self,
        order_id: str,
        status: OrderStatus,
        filled_qty: int | None = None,
        avg_price: float | None = None,
        venue_order_id: str | None = None,
        route_id: str | None = None,
        timestamp: datetime | None = None,
    ) -> OrderRecord | None:
        with self._lock:
            rec = self._records.get(order_id)
            if rec is None:
                return None
            rec.status = status
            if filled_qty is not None:
                rec.filled_qty = filled_qty
            if avg_price is not None:
                rec.avg_price = avg_price
            if venue_order_id is not None:
                rec.venue_order_id = venue_order_id
            if route_id is not None:
                rec.route_id = route_id
            if timestamp is not None:
                rec.last_update = timestamp
            return rec

    def all(self) -> list[OrderRecord]:
        with self._lock:
            return list(self._records.values())

    def by_venue_id(self, venue_order_id: str) -> OrderRecord | None:
        with self._lock:
            for rec in self._records.values():
                if rec.venue_order_id == venue_order_id:
                    return rec
            return None

    def resolve(self, order_id: str) -> OrderRecord | None:
        """Resolve by internal id first, then by venue id.

        Lets a handler look up the lifecycle entry regardless of whether the
        adapter reports the internal id (paper) or the venue id (EMSX) in
        ``ExecutionUpdate.order_id``.
        """
        rec = self.get(order_id)
        return rec if rec is not None else self.by_venue_id(order_id)

    def set_venue_info(
        self,
        order_id: str,
        venue_order_id: str | None = None,
        route_id: str | None = None,
    ) -> OrderRecord | None:
        """Update venue identifiers only. Does NOT touch ``status`` or fill data,
        so callbacks that have already advanced the order are not regressed."""
        with self._lock:
            rec = self._records.get(order_id)
            if rec is None:
                return None
            if venue_order_id is not None:
                rec.venue_order_id = venue_order_id
            if route_id is not None:
                rec.route_id = route_id
            return rec
