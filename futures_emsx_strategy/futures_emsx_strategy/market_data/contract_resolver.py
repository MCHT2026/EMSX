"""Resolves a logical instrument like 'ES1' (front month) to a concrete Bloomberg topic.

A real production setup would look up the current front contract from Bloomberg
reference data (FUT_CUR_GEN_TICKER / CRNCY / LAST_TRADEABLE_DT). For now, we
provide a static mapping in instruments.yaml plus a hook for dynamic resolution.
"""
from __future__ import annotations

from datetime import date

from ..config.loader import InstrumentConfig, InstrumentsConfig
from ..core.exceptions import ConfigError


class ContractResolver:
    def __init__(self, instruments: InstrumentsConfig) -> None:
        self._instruments = instruments

    def resolve(self, symbol: str, as_of: date | None = None) -> InstrumentConfig:
        return self._instruments.by_symbol(symbol)

    def should_roll(self, ins: InstrumentConfig, as_of: date, expiry: date) -> bool:
        days = (expiry - as_of).days
        return days <= ins.roll_days_before_expiry

    def known_symbols(self) -> list[str]:
        return [i.symbol for i in self._instruments.instruments]

    def validate(self, symbol: str) -> None:
        if symbol not in self.known_symbols():
            raise ConfigError(f"Unknown instrument: {symbol}")
