"""Strategy interface. Strategies consume BarClosed and read PortfolioView, emit TargetPosition."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Protocol

from ..core.events import BarClosed, TargetPosition


class PortfolioView(Protocol):
    """Read-only view of the portfolio that strategies are allowed to see."""

    def position(self, instrument: str) -> int: ...
    def avg_cost(self, instrument: str) -> float | None: ...


class Strategy(ABC):
    strategy_id: str

    @abstractmethod
    def on_bar(self, bar: BarClosed, portfolio: PortfolioView) -> list[TargetPosition]: ...

    def warmup(self, bars: list[BarClosed]) -> None:
        """Optional pre-load of historical bars for stateful indicators."""
        for b in bars:
            self.on_bar(b, _EmptyPortfolio())


class _EmptyPortfolio:
    def position(self, instrument: str) -> int:
        return 0

    def avg_cost(self, instrument: str) -> float | None:
        return None
