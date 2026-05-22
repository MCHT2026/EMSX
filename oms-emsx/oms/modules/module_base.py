"""Template / placeholder for user strategy modules.

A user strategy is just a ``BaseModule`` subclass that:
  - subscribes to whatever inputs it needs (market data, fills, positions),
  - calls ``self.publish("orders.new", {...})`` to send an order — the
    risk gate is the only thing standing between this and EMSX,
  - emits heartbeats automatically (handled by ``BaseModule``).

Copy this file to ``modules/strategy_<name>.py`` and edit. ``start_all.sh``
will auto-launch any file matching ``modules/strategy_*.py``.

Example::

    from modules.module_base import StrategyModule

    class MyStrategy(StrategyModule):
        async def on_fill(self, env, _mid):
            ...  # react to fills.partial / fills.done

        async def run(self):
            await self.subscribe("fills.*",        self.on_fill)
            await self.subscribe("market.price.*", self.on_price)
            await self.on_start()
            # ... main loop ...
"""
from __future__ import annotations

import asyncio
import logging
import uuid

from core.base_module import BaseModule

log = logging.getLogger(__name__)


class StrategyModule(BaseModule):
    """Convenience base class for user-defined strategies.

    Provides a single helper, ``submit_order``, that builds the canonical
    ``orders.new`` payload (with a fresh ``order_id``) and publishes it.
    """

    async def submit_order(
        self,
        instrument: str,
        side: str,
        qty: float,
        *,
        order_type: str = "LIMIT",
        limit_price: float | None = None,
        exec_style: str = "vwap",
        broker: str = "GSCO",
        account: str = "ACC001",
    ) -> str:
        order_id = str(uuid.uuid4())
        payload = {
            "order_id":   order_id,
            "instrument": instrument,
            "side":       side.upper(),
            "qty":        qty,
            "order_type": order_type,
            "exec_style": exec_style,
            "broker":     broker,
            "account":    account,
        }
        if limit_price is not None:
            payload["limit_price"] = limit_price
        await self.publish("orders.new", payload)
        return order_id

    async def run(self) -> None:  # pragma: no cover
        """Override in subclasses. Don't forget to call ``self.on_start()``."""
        await self.on_start()
        await asyncio.Event().wait()
