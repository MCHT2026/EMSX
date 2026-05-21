"""Commission models."""
from __future__ import annotations

from abc import ABC, abstractmethod


class CommissionModel(ABC):
    @abstractmethod
    def per_fill(self, qty: int, price: float, instrument: str) -> float: ...


class FlatCommissionModel(CommissionModel):
    def __init__(self, per_contract: float = 1.25) -> None:
        self.per_contract = per_contract

    def per_fill(self, qty: int, price: float, instrument: str) -> float:
        return abs(qty) * self.per_contract
