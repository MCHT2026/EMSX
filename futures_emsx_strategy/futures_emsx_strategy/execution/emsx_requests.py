"""Builds and dispatches request/response operations against EMSX.

Wraps BLPAPI's session.createRequest / session.sendRequest pattern. The
parsed responses are returned as plain dicts so emsx_mapper.py can translate.
"""
from __future__ import annotations

from typing import Any

from ..core.exceptions import EMSXError
from ..core.logging import get_logger

log = get_logger(__name__)

try:
    import blpapi
    _HAVE_BLPAPI = True
except ImportError:
    blpapi = None
    _HAVE_BLPAPI = False


class EMSXRequests:
    """Thin wrapper over an opened EMSX service."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8194,
        service: str = "//blp/emapisvc_beta",
    ) -> None:
        if not _HAVE_BLPAPI:
            raise EMSXError("blpapi not installed")
        self.host = host
        self.port = port
        self.service_name = service
        self._session: "blpapi.Session | None" = None
        self._service: "blpapi.Service | None" = None

    def start(self) -> None:
        opts = blpapi.SessionOptions()
        opts.setServerHost(self.host)
        opts.setServerPort(self.port)
        self._session = blpapi.Session(opts)
        if not self._session.start():
            raise EMSXError(f"Failed to start EMSX session to {self.host}:{self.port}")
        if not self._session.openService(self.service_name):
            raise EMSXError(f"Failed to open EMSX service {self.service_name}")
        self._service = self._session.getService(self.service_name)
        log.info("emsx_session_started", host=self.host, port=self.port)

    def stop(self) -> None:
        if self._session is not None:
            self._session.stop()
        log.info("emsx_session_stopped")

    def create_order_and_route(self, fields: dict[str, Any]) -> dict[str, Any]:
        return self._send("CreateOrderAndRouteEx", fields)

    def create_order(self, fields: dict[str, Any]) -> dict[str, Any]:
        return self._send("CreateOrder", fields)

    def route_ex(self, fields: dict[str, Any]) -> dict[str, Any]:
        return self._send("RouteEx", fields)

    def modify_route(self, fields: dict[str, Any]) -> dict[str, Any]:
        return self._send("ModifyRouteEx", fields)

    def cancel_route(self, fields: dict[str, Any]) -> dict[str, Any]:
        return self._send("CancelRouteEx", fields)

    def _send(self, op_name: str, fields: dict[str, Any]) -> dict[str, Any]:
        if self._session is None or self._service is None:
            raise EMSXError("EMSX session not started")
        req = self._service.createRequest(op_name)
        for k, v in fields.items():
            if v is None:
                continue
            try:
                req.set(k, v)
            except Exception as e:  # noqa: BLE001
                log.warning("emsx_field_set_failed", field=k, value=v, error=str(e))
        log.info("emsx_request_sent", op=op_name, fields=list(fields.keys()))
        self._session.sendRequest(req)
        return self._read_response(op_name)

    def _read_response(self, op_name: str) -> dict[str, Any]:
        assert self._session is not None
        out: dict[str, Any] = {}
        while True:
            event = self._session.nextEvent(5000)
            for msg in event:
                out.update(self._msg_to_dict(msg))
            if event.eventType() == blpapi.Event.RESPONSE:
                break
        log.info("emsx_response", op=op_name, keys=list(out.keys()))
        return out

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
