"""Builds minute OHLCV bars from a stream of trade ticks.

Polling-based bar requests have ambiguous timing — this builder uses event
timestamps to close bars deterministically. Bars close on the first tick
whose minute (floored) is greater than the open bar's minute, which means
the latest possible close lag is the next tick's arrival.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from ..core.events import BarClosed, MarketTick
from ..core.logging import get_logger

log = get_logger(__name__)


@dataclass
class _OpenBar:
    instrument: str
    start_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

    def update(self, price: float, vol: int) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += vol


BarCallback = Callable[[BarClosed], None]


class MinuteBarBuilder:
    def __init__(self, interval_minutes: int = 1) -> None:
        if interval_minutes <= 0:
            raise ValueError("interval_minutes must be positive")
        self.interval = timedelta(minutes=interval_minutes)
        self.interval_minutes = interval_minutes
        self._open_bars: dict[str, _OpenBar] = {}
        self._callbacks: list[BarCallback] = []

    def on_bar(self, callback: BarCallback) -> None:
        self._callbacks.append(callback)

    def on_tick(self, tick: MarketTick) -> None:
        price = tick.last if tick.last is not None else self._mid(tick)
        if price is None:
            return
        ts = tick.exchange_timestamp or tick.receive_timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        bar_start = self._floor(ts)
        vol = tick.volume or 0
        open_bar = self._open_bars.get(tick.instrument)
        if open_bar is None:
            self._open_bars[tick.instrument] = _OpenBar(
                instrument=tick.instrument,
                start_time=bar_start,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=vol,
            )
            return
        if bar_start > open_bar.start_time:
            self._emit(open_bar)
            self._open_bars[tick.instrument] = _OpenBar(
                instrument=tick.instrument,
                start_time=bar_start,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=vol,
            )
        else:
            open_bar.update(price, vol)

    def flush(self, instrument: str | None = None) -> None:
        if instrument is None:
            keys = list(self._open_bars.keys())
        else:
            keys = [instrument] if instrument in self._open_bars else []
        for key in keys:
            self._emit(self._open_bars.pop(key))

    def _emit(self, bar: _OpenBar) -> None:
        closed = BarClosed(
            instrument=bar.instrument,
            start_time=bar.start_time,
            end_time=bar.start_time + self.interval,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            interval_minutes=self.interval_minutes,
        )
        for cb in self._callbacks:
            try:
                cb(closed)
            except Exception:  # noqa: BLE001
                log.exception("bar_callback_failed", instrument=bar.instrument)

    def _floor(self, ts: datetime) -> datetime:
        minute_block = (ts.minute // self.interval_minutes) * self.interval_minutes
        return ts.replace(minute=minute_block, second=0, microsecond=0)

    @staticmethod
    def _mid(tick: MarketTick) -> float | None:
        if tick.bid is not None and tick.ask is not None:
            return (tick.bid + tick.ask) / 2.0
        return None
