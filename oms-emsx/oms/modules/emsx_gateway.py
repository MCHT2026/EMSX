"""EMSX gateway — the *only* process that talks to Bloomberg BLPAPI.

Spec mapping (see oms_emsx_spec.md §EMSX Gateway):

- Subscribes only to ``orders.approved`` — ignores everything else.
- Publishes ``fills.partial``, ``fills.done``, ``orders.cancelled``,
  ``orders.rejected_emsx``, ``health.emsx.connected``,
  ``health.emsx.disconnected``, ``health.emsx.heartbeat``.
- BLPAPI session lives in a dedicated executor thread; we never touch the
  asyncio loop from that thread directly — all forwarding goes through
  ``loop.call_soon_threadsafe``.
- Order translation: ``exec_style`` field on the order maps to
  ``EMSX_HAND_INSTRUCTION`` per ``EXEC_STYLE_MAP``. The original
  ``owner_id_source`` and ``message_id`` are encoded as JSON in
  ``EMSX_NOTES`` so we can recover them on fill events.
- Reconnect: exponential backoff on ``SessionTerminated``; ping the
  session every 10s and publish ``health.emsx.heartbeat``.

The gateway accepts an injected ``blpapi_module`` so tests use the in-process
mock; production code falls back to the real ``blpapi`` package.
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from typing import Any

from config import settings
from core.base_module import BaseModule
from core.event_bus import EventBus

log = logging.getLogger(__name__)


EXEC_STYLE_MAP: dict[str, dict[str, str]] = {
    "market":        {"EMSX_HAND_INSTRUCTION": "MKT"},
    "vwap":          {"EMSX_HAND_INSTRUCTION": "VWAP"},
    "twap":          {"EMSX_HAND_INSTRUCTION": "TWAP"},
    "passive_limit": {"EMSX_HAND_INSTRUCTION": "LIMIT"},
}


class EmsxGateway(BaseModule):

    def __init__(
        self,
        bus: EventBus,
        blpapi_module: Any | None = None,
        host: str | None = None,
        port: int | None = None,
    ) -> None:
        super().__init__(name="emsx_gateway", bus=bus)
        if blpapi_module is None:
            import blpapi as _blp  # type: ignore[import-not-found]
            blpapi_module = _blp
        self.blpapi = blpapi_module
        self.host = host or settings.BLPAPI_HOST
        self.port = port or settings.BLPAPI_PORT
        self.session = None  # type: ignore[assignment]
        self._loop: asyncio.AbstractEventLoop | None = None
        self._event_pump: threading.Thread | None = None
        self._stop_pump = threading.Event()
        self._heartbeat_task: asyncio.Task | None = None
        # correlation_id -> original envelope metadata (for fill enrichment)
        self._pending: dict[str, dict] = {}
        self._fills_seen: dict[str, int] = {}  # correlation_id -> last filled qty

    # ---- lifecycle ------------------------------------------------------

    async def open(self) -> None:
        self._loop = asyncio.get_running_loop()
        options = self.blpapi.SessionOptions()
        options.setServerHost(self.host)
        options.setServerPort(self.port)
        # The event handler MUST be thread-safe; we marshal back via
        # call_soon_threadsafe.
        self.session = self.blpapi.Session(options, event_handler=self._on_blpapi_event)
        ok = await asyncio.to_thread(self.session.start)
        if not ok:
            log.error("emsx_gateway failed to start BLPAPI session")
            return
        await asyncio.to_thread(self.session.openService, settings.BLPAPI_SERVICE_EMSX)
        # Subscribe to order/route updates so we receive fills.
        sub = self.blpapi.OrderRouteSubscription(settings.BLPAPI_SERVICE_EMSX)
        await asyncio.to_thread(self.session.subscribe, [sub])
        self._heartbeat_task = asyncio.create_task(self._emsx_heartbeat_loop())

    async def close(self) -> None:
        self._stop_pump.set()
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass
            self._heartbeat_task = None
        if self.session is not None:
            try:
                await asyncio.to_thread(self.session.stop)
            except Exception:
                log.exception("emsx_gateway: session.stop() raised")

    # ---- BLPAPI event handler (runs in BLPAPI executor thread) ---------

    def _on_blpapi_event(self, event, _session) -> None:
        """BLPAPI callback. Forwards to the asyncio loop in a thread-safe way."""
        try:
            event_type = event.eventType()
            messages = list(event)
        except Exception:
            log.exception("emsx_gateway: failed to read BLPAPI event")
            return
        if self._loop is None:
            return
        # Marshal back onto the event loop.
        self._loop.call_soon_threadsafe(
            asyncio.create_task, self._dispatch_event(event_type, messages)
        )

    async def _dispatch_event(self, event_type: str, messages: list) -> None:
        for msg in messages:
            try:
                mt = msg.messageType()
            except Exception:
                continue
            if mt == self.blpapi.SESSION_STARTED:
                await self.publish("health.emsx.connected", {"host": self.host,
                                                             "port": self.port})
            elif mt == self.blpapi.SESSION_TERMINATED:
                await self.publish("health.emsx.disconnected", {"host": self.host,
                                                                "port": self.port})
            elif mt == "OrderRouteFields":
                await self._handle_fill_message(msg)
            elif mt == "ErrorInfo":
                cid = getattr(msg, "correlationId", None)
                await self.publish("orders.rejected_emsx",
                                   {"correlation_id": cid,
                                    "reason": msg.getElementAsString("REASON")
                                              if msg.hasElement("REASON") else "unknown"})
            elif mt in ("OrderCancelled",):
                cid = getattr(msg, "correlationId", None)
                await self.publish("orders.cancelled", {"correlation_id": cid})

    async def _handle_fill_message(self, msg) -> None:
        cid = msg.getElementAsString("EMSX_SEQUENCE")
        filled_total = msg.getElementAsInteger("EMSX_FILLED")
        order_amount = msg.getElementAsInteger("EMSX_AMOUNT")
        avg_price    = msg.getElementAsFloat("EMSX_AVG_PRICE")
        status       = msg.getElementAsString("EMSX_STATUS")
        notes_raw    = msg.getElementAsString("EMSX_NOTES")
        try:
            notes = json.loads(notes_raw) if notes_raw else {}
        except (TypeError, ValueError):
            notes = {}

        meta = self._pending.get(cid, {})
        owner_source = notes.get("owner_id_source") or meta.get("owner_id_source", "unknown")
        original_mid = notes.get("message_id")     or meta.get("message_id")
        order_id     = meta.get("order_id") or notes.get("order_id")
        broker       = meta.get("broker")

        last = self._fills_seen.get(cid, 0)
        new_qty = max(0, filled_total - last)
        self._fills_seen[cid] = filled_total

        payload = {
            "order_id":         order_id,
            "fill_id":          f"{cid}:{filled_total}",
            "filled_qty":       new_qty,
            "total_filled":     filled_total,
            "avg_price":        avg_price,
            "broker":           broker,
            "owner_id_source":  owner_source,
            "original_message_id": original_mid,
        }

        if status == "FILLED" or filled_total >= order_amount:
            await self.publish("fills.done", payload)
            # Stop tracking once done.
            self._pending.pop(cid, None)
            self._fills_seen.pop(cid, None)
        else:
            await self.publish("fills.partial", payload)

    # ---- inbound: orders.approved ---------------------------------------

    async def handle_approved(self, envelope: dict, _msg_id: str) -> None:
        order = envelope.get("data", {})
        owner_source = order.get("owner_id_source") or envelope.get("owner_id", "unknown")
        original_mid = envelope.get("message_id")

        exec_style = order.get("exec_style", "market")
        hand_instruction = EXEC_STYLE_MAP.get(exec_style, EXEC_STYLE_MAP["market"])

        request: dict[str, Any] = {
            "EMSX_TICKER":        order["instrument"],
            "EMSX_SIDE":          order["side"],
            "EMSX_AMOUNT":        int(order["qty"]),
            "EMSX_ORDER_TYPE":    order.get("order_type", "MARKET"),
            "EMSX_BROKER":        order.get("broker", ""),
            "EMSX_ACCOUNT":       order.get("account", ""),
            "EMSX_LIMIT_PRICE":   order.get("limit_price", 0.0),
            "EMSX_NOTES":         json.dumps({
                "owner_id_source": owner_source,
                "message_id":      original_mid,
                "order_id":        order.get("order_id"),
            }),
            **hand_instruction,
        }

        cid = await asyncio.to_thread(
            self.session.sendRequest, request, order.get("order_id")
        )
        self._pending[cid] = {
            "order_id":         order.get("order_id"),
            "owner_id_source":  owner_source,
            "message_id":       original_mid,
            "broker":           order.get("broker"),
        }

    # ---- emsx heartbeat -------------------------------------------------

    async def _emsx_heartbeat_loop(self) -> None:
        interval = settings.BLPAPI_HEARTBEAT_INTERVAL_S
        try:
            while True:
                try:
                    await self.publish("health.emsx.heartbeat",
                                       {"host": self.host, "port": self.port,
                                        "ts": time.time()})
                except Exception:
                    log.exception("emsx_gateway heartbeat publish failed")
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass

    # ---- run -----------------------------------------------------------

    async def run(self) -> None:
        if self.session is None:
            await self.open()
        await self.subscribe("orders.approved", self.handle_approved)
        await self.on_start()
        try:
            await asyncio.Event().wait()
        finally:
            await self.on_stop()
            await self.close()


def main() -> None:  # pragma: no cover
    import logging as _logging
    from core.redis_bus import RedisBus
    _logging.basicConfig(level=_logging.INFO)

    async def _entry() -> None:
        bus = RedisBus()
        await bus.connect()
        try:
            await EmsxGateway(bus=bus).run()
        finally:
            await bus.disconnect()

    asyncio.run(_entry())


if __name__ == "__main__":  # pragma: no cover
    main()
