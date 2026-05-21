"""Bloomberg BLPAPI implementation of MarketDataProvider.

Soft-imports blpapi so the rest of the codebase runs in dev environments
without a Bloomberg install. Mirrors the dispatch pattern used by BLPAPI:
a single event loop thread pulls events off the session and dispatches
SUBSCRIPTION_DATA events to registered callbacks.
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Iterable

from ..core.events import BarClosed, MarketTick
from ..core.exceptions import MarketDataError
from ..core.logging import get_logger
from .base import MarketDataProvider, TickCallback

log = get_logger(__name__)

try:
    import blpapi
    _HAVE_BLPAPI = True
except ImportError:
    blpapi = None
    _HAVE_BLPAPI = False


class BloombergMarketDataProvider(MarketDataProvider):
    """Real Bloomberg subscription provider.

    Field semantics map onto Bloomberg's BLPAPI: BID, ASK, LAST_PRICE, VOLUME, EVT_TIME_STAMP, etc.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8194,
        service: str = "//blp/mktdata",
        historical_service: str = "//blp/refdata",
        topic_to_symbol: dict[str, str] | None = None,
    ) -> None:
        """``topic_to_symbol`` lets the caller subscribe under Bloomberg topics
        (e.g. ``ESM6 Index``) while reporting ticks under the application's
        internal instrument key (e.g. ``ES1``). When omitted, the topic is
        used verbatim as the instrument."""
        if not _HAVE_BLPAPI:
            raise MarketDataError(
                "blpapi is not installed. Install with: pip install futures_emsx_strategy[bloomberg]"
            )
        self.host = host
        self.port = port
        self.service = service
        self.historical_service = historical_service
        self._topic_to_symbol: dict[str, str] = dict(topic_to_symbol or {})
        self._session: "blpapi.Session | None" = None
        self._callbacks: list[TickCallback] = []
        self._sub_list: "blpapi.SubscriptionList | None" = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        opts = blpapi.SessionOptions()
        opts.setServerHost(self.host)
        opts.setServerPort(self.port)
        self._session = blpapi.Session(opts)
        if not self._session.start():
            raise MarketDataError(f"Failed to start BLPAPI session to {self.host}:{self.port}")
        if not self._session.openService(self.service):
            raise MarketDataError(f"Failed to open {self.service}")
        log.info("bloomberg_session_started", host=self.host, port=self.port, service=self.service)
        self._thread = threading.Thread(target=self._event_loop, name="blpapi-events", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._session is not None:
            self._session.stop()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        log.info("bloomberg_session_stopped")

    def subscribe(self, instruments: list[str], fields: list[str]) -> None:
        if self._session is None:
            raise MarketDataError("session not started")
        sub = blpapi.SubscriptionList()
        for inst in instruments:
            sub.add(inst, ",".join(fields), "", blpapi.CorrelationId(inst))
        self._session.subscribe(sub)
        self._sub_list = sub
        log.info("bloomberg_subscribed", instruments=instruments, fields=fields)

    def on_tick(self, callback: TickCallback) -> None:
        self._callbacks.append(callback)

    def _event_loop(self) -> None:
        assert self._session is not None
        while not self._stop_event.is_set():
            event = self._session.nextEvent(500)
            event_type = event.eventType()
            if event_type == blpapi.Event.SUBSCRIPTION_DATA:
                for msg in event:
                    self._dispatch_tick(msg)
            elif event_type in (blpapi.Event.SESSION_STATUS, blpapi.Event.SUBSCRIPTION_STATUS):
                for msg in event:
                    log.info("bloomberg_status", msg=str(msg))

    def _dispatch_tick(self, msg) -> None:  # type: ignore[no-untyped-def]
        topic = str(msg.correlationIds()[0].value())
        instrument = self._topic_to_symbol.get(topic, topic)
        bid = self._get_field(msg, "BID")
        ask = self._get_field(msg, "ASK")
        last = self._get_field(msg, "LAST_PRICE")
        vol = self._get_field(msg, "VOLUME")
        exch_ts = self._get_field(msg, "EVT_TIME_STAMP", as_time=True)
        tick = MarketTick(
            instrument=instrument,
            bid=float(bid) if bid is not None else None,
            ask=float(ask) if ask is not None else None,
            last=float(last) if last is not None else None,
            volume=int(vol) if vol is not None else None,
            exchange_timestamp=exch_ts,
            receive_timestamp=datetime.now(timezone.utc),
        )
        for cb in self._callbacks:
            try:
                cb(tick)
            except Exception:  # noqa: BLE001
                log.exception("tick_callback_failed", instrument=instrument)

    @staticmethod
    def _get_field(msg, name: str, as_time: bool = False):  # type: ignore[no-untyped-def]
        if not msg.hasElement(name):
            return None
        elem = msg.getElement(name)
        if as_time:
            try:
                return elem.getValueAsDatetime()
            except Exception:  # noqa: BLE001
                return None
        return elem.getValue()

    def request_historical_bars(
        self,
        instrument: str,
        start: datetime,
        end: datetime,
        interval_minutes: int = 1,
    ) -> Iterable[BarClosed]:
        if self._session is None:
            raise MarketDataError("session not started")
        if not self._session.openService(self.historical_service):
            raise MarketDataError(f"Failed to open {self.historical_service}")
        ref = self._session.getService(self.historical_service)
        req = ref.createRequest("IntradayBarRequest")
        req.set("security", instrument)
        req.set("eventType", "TRADE")
        req.set("interval", interval_minutes)
        req.set("startDateTime", start)
        req.set("endDateTime", end)
        self._session.sendRequest(req)
        bars: list[BarClosed] = []
        while True:
            event = self._session.nextEvent(5000)
            for msg in event:
                if not msg.hasElement("barData"):
                    continue
                bar_data = msg.getElement("barData").getElement("barTickData")
                for i in range(bar_data.numValues()):
                    b = bar_data.getValueAsElement(i)
                    start = b.getElementAsDatetime("time")
                    bars.append(
                        BarClosed(
                            instrument=instrument,
                            start_time=start,
                            end_time=start + timedelta(minutes=interval_minutes),
                            open=b.getElementAsFloat("open"),
                            high=b.getElementAsFloat("high"),
                            low=b.getElementAsFloat("low"),
                            close=b.getElementAsFloat("close"),
                            volume=int(b.getElementAsFloat("volume")),
                            interval_minutes=interval_minutes,
                        )
                    )
            if event.eventType() == blpapi.Event.RESPONSE:
                break
        return bars
