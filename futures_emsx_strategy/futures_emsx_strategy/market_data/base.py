"""Abstract market data provider interface.

The strategy never depends on this directly — it consumes BarClosed events from the bus.
Swapping Bloomberg for a CME-direct or vendor feed means writing a new subclass here.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable, Iterable

from ..core.events import BarClosed, MarketTick

TickCallback = Callable[[MarketTick], None]


class MarketDataProvider(ABC):
    @abstractmethod
    def start(self) -> None: ...

    @abstractmethod
    def stop(self) -> None: ...

    @abstractmethod
    def subscribe(self, instruments: list[str], fields: list[str]) -> None: ...

    @abstractmethod
    def on_tick(self, callback: TickCallback) -> None: ...

    @abstractmethod
    def request_historical_bars(
        self,
        instrument: str,
        start: datetime,
        end: datetime,
        interval_minutes: int = 1,
    ) -> Iterable[BarClosed]: ...
