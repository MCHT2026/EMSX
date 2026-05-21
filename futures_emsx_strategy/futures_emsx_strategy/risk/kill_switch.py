"""Global kill switch. When tripped, every order is rejected."""
from __future__ import annotations

from datetime import datetime
from threading import Lock

from ..core.clock import Clock, SystemClock
from ..core.enums import KillSwitchState
from ..core.events import KillSwitchEvent
from ..core.logging import get_logger

log = get_logger(__name__)


class KillSwitch:
    def __init__(self, armed: bool = True, clock: Clock | None = None) -> None:
        self._state = KillSwitchState.ARMED if armed else KillSwitchState.TRIPPED
        self._reason: str | None = None
        self._tripped_at: datetime | None = None
        self._lock = Lock()
        self._clock = clock or SystemClock()

    @property
    def is_tripped(self) -> bool:
        with self._lock:
            return self._state is KillSwitchState.TRIPPED

    def trip(self, reason: str, actor: str = "system") -> KillSwitchEvent:
        with self._lock:
            self._state = KillSwitchState.TRIPPED
            self._reason = reason
            self._tripped_at = self._clock.now()
        log.error("kill_switch_tripped", reason=reason, actor=actor)
        return KillSwitchEvent(
            tripped=True,
            reason=reason,
            timestamp=self._tripped_at or self._clock.now(),
            actor=actor,
        )

    def arm(self, actor: str = "operator") -> KillSwitchEvent:
        with self._lock:
            self._state = KillSwitchState.ARMED
            self._reason = None
        log.warning("kill_switch_armed", actor=actor)
        return KillSwitchEvent(
            tripped=False,
            reason="re-armed",
            timestamp=self._clock.now(),
            actor=actor,
        )

    @property
    def reason(self) -> str | None:
        with self._lock:
            return self._reason
