"""In-process fake of EMSXRequests + EMSXSubscriptions.

Lets us exercise EMSXExecutionAdapter end-to-end without a real Bloomberg connection.
"""
from __future__ import annotations

import itertools
import threading
from typing import Any, Callable


class FakeEMSXRequests:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._seq = itertools.count(1000)

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def create_order_and_route(self, fields: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(("CreateOrderAndRouteEx", fields))
        seq = next(self._seq)
        return {
            "EMSX_SEQUENCE": seq,
            "EMSX_ROUTE_ID": 1,
            "EMSX_STATUS": "OK",
            "MESSAGE": "Order created",
        }

    def create_order(self, fields: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(("CreateOrder", fields))
        return {
            "EMSX_SEQUENCE": next(self._seq),
            "EMSX_STATUS": "OK",
            "MESSAGE": "Order staged",
        }

    def route_ex(self, fields: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(("RouteEx", fields))
        return {"EMSX_STATUS": "OK", "MESSAGE": "Routed"}

    def modify_route(self, fields: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(("ModifyRouteEx", fields))
        return {"EMSX_STATUS": "OK", "MESSAGE": "Modified"}

    def cancel_route(self, fields: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(("CancelRouteEx", fields))
        return {"EMSX_STATUS": "OK", "MESSAGE": "Cancelled"}


class FakeEMSXSubscriptions:
    def __init__(self) -> None:
        self._order_cbs: list[Callable[[dict], None]] = []
        self._route_cbs: list[Callable[[dict], None]] = []
        self._lock = threading.Lock()

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def on_order_update(self, cb: Callable[[dict], None]) -> None:
        self._order_cbs.append(cb)

    def on_route_update(self, cb: Callable[[dict], None]) -> None:
        self._route_cbs.append(cb)

    def push_route(self, msg: dict) -> None:
        with self._lock:
            for cb in list(self._route_cbs):
                cb(msg)

    def push_order(self, msg: dict) -> None:
        with self._lock:
            for cb in list(self._order_cbs):
                cb(msg)
