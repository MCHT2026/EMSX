"""Market session validation. Refuses orders outside the configured exchange session."""
from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo

from ..config.loader import InstrumentConfig


class SessionChecker:
    def __init__(self, instrument: InstrumentConfig) -> None:
        self.instrument = instrument
        self.tz = ZoneInfo(instrument.session_tz)
        self.open_t = self._parse(instrument.session_open)
        self.close_t = self._parse(instrument.session_close)

    @staticmethod
    def _parse(s: str) -> time:
        h, m = s.split(":")
        return time(int(h), int(m))

    def is_open(self, now_utc: datetime) -> bool:
        local = now_utc.astimezone(self.tz).time()
        if self.open_t <= self.close_t:
            return self.open_t <= local <= self.close_t
        return local >= self.open_t or local <= self.close_t
