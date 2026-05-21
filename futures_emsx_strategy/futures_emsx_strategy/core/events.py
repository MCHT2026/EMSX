"""Canonical event types passed across services on the bus."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .enums import OrderStatus, OrderType, Side, TimeInForce


@dataclass(slots=True, frozen=True)
class MarketTick:
    instrument: str
    bid: float | None
    ask: float | None
    last: float | None
    volume: int | None
    exchange_timestamp: datetime | None
    receive_timestamp: datetime


@dataclass(slots=True, frozen=True)
class BarClosed:
    instrument: str
    start_time: datetime
    end_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    interval_minutes: int = 1


@dataclass(slots=True, frozen=True)
class TargetPosition:
    strategy_id: str
    instrument: str
    target_qty: int
    timestamp: datetime
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class OrderIntent:
    strategy_id: str
    instrument: str
    side: Side
    qty: int
    order_type: OrderType
    time_in_force: TimeInForce
    idempotency_key: str
    source_timestamp: datetime
    limit_price: float | None = None
    stop_price: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class RiskDecision:
    order_key: str
    approved: bool
    reasons: tuple[str, ...]
    decided_at: datetime

    @classmethod
    def approve(cls, order_key: str, decided_at: datetime) -> "RiskDecision":
        return cls(order_key=order_key, approved=True, reasons=(), decided_at=decided_at)

    @classmethod
    def reject(cls, order_key: str, reasons: list[str], decided_at: datetime) -> "RiskDecision":
        return cls(order_key=order_key, approved=False, reasons=tuple(reasons), decided_at=decided_at)


@dataclass(slots=True, frozen=True)
class ExecutionAck:
    """Returned synchronously from the venue request/response call."""

    order_id: str
    route_id: str | None
    venue_request_id: str | None
    accepted: bool
    message: str
    timestamp: datetime


@dataclass(slots=True, frozen=True)
class ExecutionUpdate:
    """Async update from venue subscriptions (EMSX route/order events)."""

    order_id: str
    route_id: str | None
    instrument: str
    status: OrderStatus
    filled_qty: int
    avg_price: float | None
    leaves_qty: int
    timestamp: datetime
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class FillUpdate:
    order_id: str
    route_id: str | None
    instrument: str
    side: Side
    fill_qty: int
    fill_price: float
    timestamp: datetime


@dataclass(slots=True, frozen=True)
class KillSwitchEvent:
    tripped: bool
    reason: str
    timestamp: datetime
    actor: str = "system"
