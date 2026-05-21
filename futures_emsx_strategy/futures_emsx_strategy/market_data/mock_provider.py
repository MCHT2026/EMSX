"""In-memory market data provider for tests, paper trading, and backtests."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable

from ..core.events import BarClosed, MarketTick
from .base import MarketDataProvider, TickCallback


class MockMarketDataProvider(MarketDataProvider):
    def __init__(self) -> None:
        self._callbacks: list[TickCallback] = []
        self._started = False
        self._subscribed: list[str] = []

    def start(self) -> None:
        self._started = True

    def stop(self) -> None:
        self._started = False

    def subscribe(self, instruments: list[str], fields: list[str]) -> None:
        self._subscribed = list(instruments)

    def on_tick(self, callback: TickCallback) -> None:
        self._callbacks.append(callback)

    def push_tick(self, tick: MarketTick) -> None:
        for cb in self._callbacks:
            cb(tick)

    def request_historical_bars(
        self,
        instrument: str,
        start: datetime,
        end: datetime,
        interval_minutes: int = 1,
    ) -> Iterable[BarClosed]:
        bars = []
        t = start
        price = 4500.0
        while t < end:
            bars.append(
                BarClosed(
                    instrument=instrument,
                    start_time=t,
                    end_time=t + timedelta(minutes=interval_minutes),
                    open=price,
                    high=price + 0.5,
                    low=price - 0.5,
                    close=price + 0.25,
                    volume=100,
                    interval_minutes=interval_minutes,
                )
            )
            t += timedelta(minutes=interval_minutes)
            price += 0.25
        return bars

    @staticmethod
    def tick(
        instrument: str,
        last: float,
        ts: datetime | None = None,
        volume: int = 1,
    ) -> MarketTick:
        ts = ts or datetime.now(timezone.utc)
        return MarketTick(
            instrument=instrument,
            bid=last - 0.25,
            ask=last + 0.25,
            last=last,
            volume=volume,
            exchange_timestamp=ts,
            receive_timestamp=ts,
        )
