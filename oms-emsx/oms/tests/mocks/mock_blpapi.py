"""In-process mock of the Bloomberg BLPAPI session.

Covers the subset of BLPAPI surface area exercised by the EMSX gateway:

- ``SessionOptions`` — host/port/serviceCheckTimeout setters
- ``Session`` — start / stop / openService / sendRequest / subscribe / nextEvent
- Event types: ``SessionStarted``, ``SessionTerminated``, ``ServiceOpened``,
  ``SubscriptionData``
- ``OrderRouteSubscription`` — represented as a subscription topic string
  (``//blp/emapisvc/order``) per BLPAPI conventions
- Synthetic fill stream with configurable delay so EMSX gateway tests can
  watch a partial-fill → done-fill cycle without waiting on a real EMSX.

Drop-in for ``import blpapi``: ``import tests.mocks.mock_blpapi as blpapi``.
"""
from __future__ import annotations

import threading
import time
import uuid
from queue import Queue, Empty
from typing import Any, Callable


# ---- event-type constants -------------------------------------------------

SESSION_STATUS = "SessionStatus"
SERVICE_STATUS = "ServiceStatus"
SUBSCRIPTION_DATA = "SubscriptionData"
SUBSCRIPTION_STATUS = "SubscriptionStatus"
RESPONSE = "Response"
PARTIAL_RESPONSE = "PartialResponse"
ADMIN = "Admin"
TIMEOUT = "Timeout"

# message types
SESSION_STARTED = "SessionStarted"
SESSION_TERMINATED = "SessionTerminated"
SERVICE_OPENED = "ServiceOpened"
SERVICE_OPEN_FAILURE = "ServiceOpenFailure"


# ---- Name (BLPAPI uses Name() objects, we accept strings interchangeably)

class Name:
    def __init__(self, s: str) -> None:
        self._s = s

    def __str__(self) -> str:
        return self._s

    def __eq__(self, other) -> bool:
        return str(self) == str(other)

    def __hash__(self) -> int:
        return hash(self._s)


# ---- Element / Message ---------------------------------------------------

class Element:
    """Minimal BLPAPI Element: a key/value tree."""

    def __init__(self, fields: dict[str, Any] | None = None) -> None:
        self._fields: dict[str, Any] = dict(fields or {})

    def setElement(self, name: str | Name, value: Any) -> None:
        self._fields[str(name)] = value

    def getElement(self, name: str | Name) -> "Element":
        v = self._fields[str(name)]
        if isinstance(v, Element):
            return v
        return Element({str(name): v})

    def getElementAsString(self, name: str | Name) -> str:
        return str(self._fields[str(name)])

    def getElementAsInteger(self, name: str | Name) -> int:
        return int(self._fields[str(name)])

    def getElementAsFloat(self, name: str | Name) -> float:
        return float(self._fields[str(name)])

    def hasElement(self, name: str | Name) -> bool:
        return str(name) in self._fields

    def asDict(self) -> dict[str, Any]:
        return dict(self._fields)


class Message:
    """Mock BLPAPI Message."""

    def __init__(self, message_type: str, fields: dict[str, Any] | None = None,
                 correlation_id: str | None = None) -> None:
        self._type = message_type
        self._fields = dict(fields or {})
        self.correlationId = correlation_id

    def messageType(self) -> str:
        return self._type

    def asElement(self) -> Element:
        return Element(self._fields)

    def hasElement(self, name: str | Name) -> bool:
        return str(name) in self._fields

    def getElement(self, name: str | Name) -> Element:
        v = self._fields[str(name)]
        if isinstance(v, Element):
            return v
        return Element({str(name): v})

    def getElementAsString(self, name: str | Name) -> str:
        return str(self._fields[str(name)])

    def getElementAsInteger(self, name: str | Name) -> int:
        return int(self._fields[str(name)])

    def getElementAsFloat(self, name: str | Name) -> float:
        return float(self._fields[str(name)])


class Event:
    """Mock BLPAPI Event with a list of messages."""

    def __init__(self, event_type: str, messages: list[Message]) -> None:
        self._type = event_type
        self._messages = list(messages)

    def eventType(self) -> str:
        return self._type

    def __iter__(self):
        return iter(self._messages)


# ---- SessionOptions ------------------------------------------------------

class SessionOptions:
    def __init__(self) -> None:
        self._host = "localhost"
        self._port = 8194
        self._service_check_timeout = 60_000

    def setServerHost(self, host: str) -> None:
        self._host = host

    def setServerPort(self, port: int) -> None:
        self._port = port

    def setServiceCheckTimeout(self, ms: int) -> None:
        self._service_check_timeout = ms


# ---- Session -------------------------------------------------------------

class Session:
    """Mock BLPAPI Session.

    Drives a synthetic fill stream on the event queue: after a successful
    ``sendRequest`` for an order, the session emits a ``PartialFill`` and
    then a ``Fill`` event for the same order, separated by ``fill_delay_s``.
    """

    # Class-level so tests can tweak globally before constructing.
    fill_delay_s: float = 0.0
    partial_fill_fraction: float = 0.5

    def __init__(
        self,
        options: SessionOptions | None = None,
        event_handler: Callable[[Event, "Session"], None] | None = None,
    ) -> None:
        self._options = options or SessionOptions()
        self._event_handler = event_handler
        self._queue: Queue[Event] = Queue()
        self._running = False
        self._services: set[str] = set()
        self._subscriptions: list[str] = []
        self._workers: list[threading.Thread] = []
        # External test hook: orders that have been "sent".
        self.sent_orders: list[dict[str, Any]] = []

    # ---- session lifecycle ----------------------------------------------

    def start(self) -> bool:
        self._running = True
        ev = Event(SESSION_STATUS,
                   [Message(SESSION_STARTED, {})])
        self._emit(ev)
        return True

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        ev = Event(SESSION_STATUS,
                   [Message(SESSION_TERMINATED, {})])
        self._emit(ev)
        for t in list(self._workers):
            t.join(timeout=2.0)

    def openService(self, service: str) -> bool:
        if not self._running:
            return False
        self._services.add(service)
        ev = Event(SERVICE_STATUS,
                   [Message(SERVICE_OPENED, {"serviceName": service})])
        self._emit(ev)
        return True

    # ---- order send + synthetic fills -----------------------------------

    def sendRequest(
        self,
        request: dict[str, Any] | Element,
        correlationId: str | None = None,
    ) -> str:
        """Record the order and schedule synthetic fills."""
        order = request.asDict() if isinstance(request, Element) else dict(request)
        order["_correlation_id"] = correlationId or str(uuid.uuid4())
        self.sent_orders.append(order)
        # Spawn a worker so emission is asynchronous like real BLPAPI.
        t = threading.Thread(
            target=self._emit_fills, args=(order,), daemon=True,
            name=f"mockblp:{order['_correlation_id']}",
        )
        self._workers.append(t)
        t.start()
        return order["_correlation_id"]

    def _emit_fills(self, order: dict[str, Any]) -> None:
        if self.fill_delay_s:
            time.sleep(self.fill_delay_s)
        qty   = int(order.get("EMSX_AMOUNT", 0))
        notes = str(order.get("EMSX_NOTES", ""))
        partial = max(1, int(qty * self.partial_fill_fraction))
        price = float(order.get("EMSX_LIMIT_PRICE", 0.0)) or 100.0

        cid = order["_correlation_id"]
        self._emit(Event(SUBSCRIPTION_DATA, [Message(
            "OrderRouteFields",
            {
                "EMSX_SEQUENCE":  cid,
                "EMSX_FILLED":    partial,
                "EMSX_AMOUNT":    qty,
                "EMSX_AVG_PRICE": price,
                "EMSX_STATUS":    "PARTFILL",
                "EMSX_NOTES":     notes,
            },
        )]))
        if self.fill_delay_s:
            time.sleep(self.fill_delay_s)
        self._emit(Event(SUBSCRIPTION_DATA, [Message(
            "OrderRouteFields",
            {
                "EMSX_SEQUENCE":  cid,
                "EMSX_FILLED":    qty,
                "EMSX_AMOUNT":    qty,
                "EMSX_AVG_PRICE": price,
                "EMSX_STATUS":    "FILLED",
                "EMSX_NOTES":     notes,
            },
        )]))

    # ---- subscribe -------------------------------------------------------

    def subscribe(self, subscription_list: list[str]) -> None:
        self._subscriptions.extend(subscription_list)
        ev = Event(SUBSCRIPTION_STATUS, [Message(
            "SubscriptionStarted",
            {"topic": ",".join(subscription_list)},
        )])
        self._emit(ev)

    # ---- event queue API -------------------------------------------------

    def _emit(self, event: Event) -> None:
        if self._event_handler is not None:
            try:
                self._event_handler(event, self)
            except Exception:
                pass
        else:
            self._queue.put(event)

    def nextEvent(self, timeout_ms: int = 100) -> Event:
        try:
            return self._queue.get(timeout=timeout_ms / 1000.0)
        except Empty:
            return Event(TIMEOUT, [])


# ---- Subscription helpers -----------------------------------------------

def OrderRouteSubscription(emsx_service: str = "//blp/emapisvc") -> str:
    """Return the canonical EMSX order/route subscription string."""
    return f"{emsx_service}/order"
