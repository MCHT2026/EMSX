"""Mapping between EMSX wire types and our internal events.

EMSX uses its own field names (EMSX_SEQUENCE, EMSX_AMOUNT, EMSX_STATUS, ...) and
status strings. This module is the single place where those names appear.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ..config.loader import EMSXConfig
from ..core.enums import OrderStatus, OrderType, RouteStatus, Side, TimeInForce
from ..core.events import ExecutionAck, ExecutionUpdate, FillUpdate, OrderIntent


_ROUTE_TO_INTERNAL: dict[str, OrderStatus] = {
    RouteStatus.SENT.value: OrderStatus.SENT,
    RouteStatus.WORKING.value: OrderStatus.WORKING,
    RouteStatus.PARTFILLED.value: OrderStatus.PART_FILLED,
    RouteStatus.FILLED.value: OrderStatus.FILLED,
    RouteStatus.CANCEL.value: OrderStatus.CANCELLED,
    RouteStatus.REJECTED.value: OrderStatus.REJECTED,
    RouteStatus.ROUTE_ERR.value: OrderStatus.ROUTE_ERR,
}


class EMSXMapper:
    def __init__(self, config: EMSXConfig) -> None:
        self.config = config

    def to_create_order_and_route(self, intent: OrderIntent) -> dict[str, Any]:
        """Fields suitable for an EMSX CreateOrderAndRouteEx request."""
        return {
            "EMSX_TICKER": intent.instrument,
            "EMSX_SIDE": self._side(intent.side),
            "EMSX_AMOUNT": intent.qty,
            "EMSX_ORDER_TYPE": self._order_type(intent.order_type),
            "EMSX_TIF": self._tif(intent.time_in_force),
            "EMSX_LIMIT_PRICE": intent.limit_price,
            "EMSX_STOP_PRICE": intent.stop_price,
            "EMSX_BROKER": self.config.broker,
            "EMSX_HAND_INSTRUCTION": self.config.handling_instruction,
            "EMSX_STRATEGY_TYPE": self.config.strategy,
            "EMSX_ACCOUNT": self.config.account,
            "EMSX_TRADER_UUID": self.config.trader_uuid,
            "EMSX_CLIENT_ORDER_ID": intent.idempotency_key,
        }

    def to_modify_route(self, route_id: str, changes: dict[str, Any]) -> dict[str, Any]:
        out = {"EMSX_SEQUENCE": int(route_id.split(":")[0])}
        for k, v in changes.items():
            if v is not None:
                out[k] = v
        return out

    def to_cancel_route(self, route_id: str) -> dict[str, Any]:
        seq_str = route_id.split(":")[0]
        route_id_part = route_id.split(":")[1] if ":" in route_id else "1"
        return {
            "EMSX_SEQUENCE": int(seq_str),
            "EMSX_ROUTE_ID": int(route_id_part),
        }

    def from_create_response(self, response: dict[str, Any], intent: OrderIntent) -> ExecutionAck:
        sequence = response.get("EMSX_SEQUENCE")
        route = response.get("EMSX_ROUTE_ID")
        accepted = response.get("EMSX_STATUS", "OK") != "ERROR"
        message = str(response.get("MESSAGE", "OK"))
        return ExecutionAck(
            order_id=str(sequence) if sequence is not None else intent.idempotency_key,
            route_id=f"{sequence}:{route}" if route is not None else None,
            venue_request_id=str(response.get("EMSX_REQUEST_SEQ", "")),
            accepted=accepted,
            message=message,
            timestamp=datetime.now(timezone.utc),
        )

    def from_route_subscription(self, msg: dict[str, Any]) -> ExecutionUpdate:
        sequence = msg.get("EMSX_SEQUENCE")
        route = msg.get("EMSX_ROUTE_ID")
        status_str = str(msg.get("EMSX_STATUS", ""))
        status = _ROUTE_TO_INTERNAL.get(status_str, OrderStatus.UNKNOWN)
        filled = int(msg.get("EMSX_FILLED", 0) or 0)
        leaves = int(msg.get("EMSX_WORKING", 0) or 0)
        avg = msg.get("EMSX_AVG_PRICE")
        return ExecutionUpdate(
            order_id=str(sequence) if sequence is not None else "?",
            route_id=f"{sequence}:{route}" if route is not None else None,
            instrument=str(msg.get("EMSX_TICKER", "")),
            status=status,
            filled_qty=filled,
            avg_price=float(avg) if avg is not None else None,
            leaves_qty=leaves,
            timestamp=datetime.now(timezone.utc),
            raw=dict(msg),
        )

    def from_fill_message(self, msg: dict[str, Any]) -> FillUpdate:
        sequence = msg.get("EMSX_SEQUENCE")
        route = msg.get("EMSX_ROUTE_ID")
        side_str = str(msg.get("EMSX_SIDE", "BUY"))
        return FillUpdate(
            order_id=str(sequence) if sequence is not None else "?",
            route_id=f"{sequence}:{route}" if route is not None else None,
            instrument=str(msg.get("EMSX_TICKER", "")),
            side=Side(side_str),
            fill_qty=int(msg.get("EMSX_FILL_AMOUNT", msg.get("EMSX_FILLED", 0)) or 0),
            fill_price=float(msg.get("EMSX_FILL_PRICE", msg.get("EMSX_AVG_PRICE", 0.0))),
            timestamp=datetime.now(timezone.utc),
        )

    @staticmethod
    def _side(side: Side) -> str:
        return "BUY" if side is Side.BUY else "SELL"

    @staticmethod
    def _order_type(ot: OrderType) -> str:
        return {
            OrderType.MKT: "MKT",
            OrderType.LMT: "LMT",
            OrderType.STP: "STP",
            OrderType.STP_LMT: "SL",
        }[ot]

    @staticmethod
    def _tif(t: TimeInForce) -> str:
        return {
            TimeInForce.DAY: "DAY",
            TimeInForce.GTC: "GTC",
            TimeInForce.IOC: "IOC",
            TimeInForce.FOK: "FOK",
            TimeInForce.GTD: "GTD",
        }[t]
