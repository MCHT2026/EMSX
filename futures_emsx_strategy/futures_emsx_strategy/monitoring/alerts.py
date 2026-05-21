"""Alert sinks. Console default; replace with Slack/PagerDuty/email in prod."""
from __future__ import annotations

from abc import ABC, abstractmethod

from ..core.logging import get_logger

log = get_logger(__name__)


class AlertSink(ABC):
    @abstractmethod
    def alert(self, severity: str, message: str, **fields) -> None: ...


class ConsoleAlertSink(AlertSink):
    def alert(self, severity: str, message: str, **fields) -> None:
        if severity in ("critical", "error"):
            log.error("alert", severity=severity, msg=message, **fields)
        elif severity == "warning":
            log.warning("alert", severity=severity, msg=message, **fields)
        else:
            log.info("alert", severity=severity, msg=message, **fields)
