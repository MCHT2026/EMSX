"""Risk limit container, sourced from risk_limits.yaml."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RiskLimits:
    max_order_qty: int
    max_position: int
    max_notional: float
    stale_data_seconds: int
    require_market_session: bool
    max_orders_per_minute: int
    max_cancels_per_minute: int
