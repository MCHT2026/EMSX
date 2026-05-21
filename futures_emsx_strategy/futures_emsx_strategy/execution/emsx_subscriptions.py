"""EMSX order/route subscriptions.

EMSX is event-driven: request/response calls mutate the EMSX book, while
subscriptions on //blp/emapisvc/order and //blp/emapisvc/route keep the
local state in sync. This module owns those subscriptions and dispatches
typed callbacks to the rest of the system.
"""
from __future__ import annotations

import threading
from typing import Any, Callable

from ..core.exceptions import EMSXError
from ..core.logging import get_logger

log = get_logger(__name__)

try:
    import blpapi
    _HAVE_BLPAPI = True
except ImportError:
    blpapi = None
    _HAVE_BLPAPI = False


MessageCallback = Callable[[dict[str, Any]], None]


class EMSXSubscriptions:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8194,
        order_topic: str = "//blp/emapisvc/order",
        route_topic: str = "//blp/emapisvc/route",
        order_fields: list[str] | None = None,
        route_fields: list[str] | None = None,
    ) -> None:
        if not _HAVE_BLPAPI:
            raise EMSXError("blpapi not installed")
        self.host = host
        self.port = port
        self.order_topic = order_topic
        self.route_topic = route_topic
        self.order_fields = order_fields or [
            "EMSX_SEQUENCE",
            "EMSX_TICKER",
            "EMSX_SIDE",
            "EMSX_AMOUNT",
            "EMSX_STATUS",
            "EMSX_FILLED",
            "EMSX_WORKING",
            "EMSX_AVG_PRICE",
        ]
        self.route_fields = route_fields or [
            "EMSX_SEQUENCE",
            "EMSX_ROUTE_ID",
            "EMSX_TICKER",
            "EMSX_SIDE",
            "EMSX_AMOUNT",
            "EMSX_STATUS",
            "EMSX_FILLED",
            "EMSX_WORKING",
            "EMSX_AVG_PRICE",
            "EMSX_FILL_AMOUNT",
            "EMSX_FILL_PRICE",
            "EMSX_LAST_FILL_DATE",
            "EMSX_LAST_FILL_TIME",
        ]
        self._session: "blpapi.Session | None" = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._order_cbs: list[MessageCallback] = []
        self._route_cbs: list[MessageCallback] = []

    def start(self) -> None:
        opts = blpapi.SessionOptions()
        opts.setServerHost(self.host)
        opts.setServerPort(self.port)
        self._session = blpapi.Session(opts)
        if not self._session.start():
            raise EMSXError("EMSX subscription session failed to start")
        sub = blpapi.SubscriptionList()
        sub.add(self.order_topic, ",".join(self.order_fields), "", blpapi.CorrelationId("order"))
        sub.add(self.route_topic, ",".join(self.route_fields), "", blpapi.CorrelationId("route"))
        self._session.subscribe(sub)
        self._thread = threading.Thread(target=self._event_loop, name="emsx-subs", daemon=True)
        self._thread.start()
        log.info("emsx_subscriptions_started")

    def stop(self) -> None:
        self._stop.set()
        if self._session is not None:
            self._session.stop()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        log.info("emsx_subscriptions_stopped")

    def on_order_update(self, cb: MessageCallback) -> None:
        self._order_cbs.append(cb)

    def on_route_update(self, cb: MessageCallback) -> None:
        self._route_cbs.append(cb)

    def _event_loop(self) -> None:
        assert self._session is not None
        while not self._stop.is_set():
            event = self._session.nextEvent(500)
            if event.eventType() != blpapi.Event.SUBSCRIPTION_DATA:
                continue
            for msg in event:
                topic = msg.correlationIds()[0].value()
                d = self._msg_to_dict(msg)
                callbacks = self._order_cbs if topic == "order" else self._route_cbs
                for cb in callbacks:
                    try:
                        cb(d)
                    except Exception:  # noqa: BLE001
                        log.exception("emsx_subscription_callback_failed", topic=topic)

    @staticmethod
    def _msg_to_dict(msg) -> dict[str, Any]:  # type: ignore[no-untyped-def]
        out: dict[str, Any] = {}
        try:
            for i in range(msg.numElements()):
                el = msg.getElement(i)
                name = el.name().__str__()
                try:
                    out[name] = el.getValue()
                except Exception:  # noqa: BLE001
                    out[name] = str(el)
        except Exception:  # noqa: BLE001
            out["_raw"] = str(msg)
        return out
