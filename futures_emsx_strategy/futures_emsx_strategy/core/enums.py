from __future__ import annotations

from enum import Enum


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

    @property
    def sign(self) -> int:
        return 1 if self is Side.BUY else -1

    @classmethod
    def from_delta(cls, delta: int) -> "Side":
        if delta == 0:
            raise ValueError("Side undefined for zero delta")
        return cls.BUY if delta > 0 else cls.SELL


class OrderType(str, Enum):
    MKT = "MKT"
    LMT = "LMT"
    STP = "STP"
    STP_LMT = "STP_LMT"


class TimeInForce(str, Enum):
    DAY = "DAY"
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    GTD = "GTD"


class OrderStatus(str, Enum):
    """Internal status, normalized across venues."""

    NEW = "NEW"
    RISK_APPROVED = "RISK_APPROVED"
    RISK_REJECTED = "RISK_REJECTED"
    SENT = "SENT"
    ACCEPTED = "ACCEPTED"
    WORKING = "WORKING"
    PART_FILLED = "PART_FILLED"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELLED = "CANCELLED"
    MODIFY_PENDING = "MODIFY_PENDING"
    REJECTED = "REJECTED"
    ROUTE_ERR = "ROUTE_ERR"
    UNKNOWN = "UNKNOWN"


class RouteStatus(str, Enum):
    SENT = "SENT"
    WORKING = "WORKING"
    PARTFILLED = "PARTFILLED"
    FILLED = "FILLED"
    CANCEL = "CANCEL"
    REJECTED = "REJECTED"
    ROUTE_ERR = "ROUTE-ERR"


class KillSwitchState(str, Enum):
    ARMED = "ARMED"
    TRIPPED = "TRIPPED"


TERMINAL_ORDER_STATUSES = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.ROUTE_ERR}
)
