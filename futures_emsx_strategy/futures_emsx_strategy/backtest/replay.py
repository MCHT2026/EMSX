"""Bar and tick replay over historical data."""
from __future__ import annotations

from datetime import datetime
from typing import Iterable, Iterator

from ..core.events import BarClosed, MarketTick


class BarReplay:
    def __init__(self, bars: Iterable[BarClosed]) -> None:
        self._bars = sorted(bars, key=lambda b: b.end_time)

    def __iter__(self) -> Iterator[BarClosed]:
        yield from self._bars

    def between(self, start: datetime, end: datetime) -> Iterator[BarClosed]:
        for b in self._bars:
            if start <= b.end_time <= end:
                yield b


class TickReplay:
    def __init__(self, ticks: Iterable[MarketTick]) -> None:
        self._ticks = sorted(ticks, key=lambda t: t.exchange_timestamp or t.receive_timestamp)

    def __iter__(self) -> Iterator[MarketTick]:
        yield from self._ticks
