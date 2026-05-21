"""Execution adapter interface. EMSX, paper, FIX — all conform to this."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable

from ..core.events import ExecutionAck, ExecutionUpdate, FillUpdate, OrderIntent

ExecutionUpdateCallback = Callable[[ExecutionUpdate], None]
FillCallback = Callable[[FillUpdate], None]


class ExecutionAdapter(ABC):
    @abstractmethod
    def start(self) -> None: ...

    @abstractmethod
    def stop(self) -> None: ...

    @abstractmethod
    def submit_order(self, order: OrderIntent) -> ExecutionAck: ...

    @abstractmethod
    def cancel_order(self, order_id: str) -> ExecutionAck: ...

    @abstractmethod
    def modify_order(self, order_id: str, changes: dict) -> ExecutionAck: ...

    @abstractmethod
    def on_execution_update(self, callback: ExecutionUpdateCallback) -> None: ...

    @abstractmethod
    def on_fill(self, callback: FillCallback) -> None: ...
