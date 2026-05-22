"""Risk Gate — mandatory pipeline stage between strategies and EMSX.

Spec mapping (see oms_emsx_spec.md §Risk Gate):

- Subscribes: orders.new, market.price.*, market.vol.*, positions.update,
  account.margin, orders.rejected (own rejections, for spike alerting).
- Publishes: orders.approved (it is the ONLY component allowed to publish
  this), orders.rejected, health.risk.rejection_spike.
- Maintains in-memory state for prices, vol, positions, margin.
- Checks in fail-fast order: notional -> position -> margin -> vol ->
  kill switch.
- Kill switch is a Redis key ``risk_gate:kill_switch`` (presence == active).
- Tracks rejection rate per owner_id over a rolling window and emits
  ``health.risk.rejection_spike`` when threshold is exceeded.

The original ``owner_id`` of the *strategy* that submitted the order is
preserved as ``owner_id_source`` inside the data payload — the envelope's
``owner_id`` itself becomes "risk_gate" since that's the publisher.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque

from config import settings
from core.base_module import BaseModule
from core.event_bus import EventBus

log = logging.getLogger(__name__)

KILL_SWITCH_KEY = "risk_gate:kill_switch"


class RiskGate(BaseModule):
    """Pre-trade risk gate.

    Parameters
    ----------
    bus
        Event bus. Required.
    max_notional, max_position, vol_threshold, margin_buffer_pct
        Risk limits. Default to ``config.settings`` values.
    """

    def __init__(
        self,
        bus: EventBus,
        max_notional: float | None = None,
        max_position: float | None = None,
        vol_threshold: float | None = None,
        margin_buffer_pct: float | None = None,
    ) -> None:
        super().__init__(name="risk_gate", bus=bus)
        self.max_notional      = max_notional      if max_notional      is not None else settings.MAX_NOTIONAL
        self.max_position      = max_position      if max_position      is not None else settings.MAX_POSITION
        self.vol_threshold     = vol_threshold     if vol_threshold     is not None else settings.VOL_THRESHOLD
        self.margin_buffer_pct = margin_buffer_pct if margin_buffer_pct is not None else settings.MARGIN_BUFFER_PCT

        self.state: dict[str, dict] = {
            "prices":     {},
            "volatility": {},
            "positions":  {},
            "margin":     {},
        }
        self.kill_switch_active = False

        # rejection spike tracking: owner_id -> deque[timestamps]
        self._rejections: dict[str, Deque[float]] = {}
        self.rejection_window_s = settings.RISK_REJECTION_WINDOW_S
        self.rejection_threshold = settings.RISK_REJECTION_THRESHOLD
        self._last_spike_alert_at: dict[str, float] = {}

    # ---- inbound state streams ------------------------------------------

    async def handle_market_price(self, envelope: dict, _mid: str) -> None:
        d = envelope.get("data", {})
        instrument = d.get("instrument")
        if not instrument:
            return
        last = d.get("last")
        if last is None:
            mid_px = self._mid_price(d.get("bid"), d.get("ask"))
            if mid_px is not None:
                self.state["prices"][instrument] = mid_px
        else:
            self.state["prices"][instrument] = last

    async def handle_market_vol(self, envelope: dict, _mid: str) -> None:
        d = envelope.get("data", {})
        instrument = d.get("instrument")
        vol = d.get("vol")
        if instrument is not None and vol is not None:
            self.state["volatility"][instrument] = vol

    async def handle_position_update(self, envelope: dict, _mid: str) -> None:
        d = envelope.get("data", {})
        account = d.get("account")
        instrument = d.get("instrument")
        if account and instrument:
            self.state["positions"][(account, instrument)] = d.get("net", 0)

    async def handle_margin_update(self, envelope: dict, _mid: str) -> None:
        d = envelope.get("data", {})
        account = d.get("account")
        if account:
            # Spec uses "account.margin" with ``available`` field.
            self.state["margin"][account] = d.get("available", d.get("margin", 0))

    # ---- kill switch ----------------------------------------------------

    async def refresh_kill_switch(self) -> None:
        """Probe the Redis key. Safe to call on every order or via background
        loop. For bus implementations without a redis attribute (in-memory
        tests) we leave the flag untouched.
        """
        redis = getattr(self.bus, "redis", None)
        if redis is None:
            return
        exists = await redis.exists(KILL_SWITCH_KEY)
        self.kill_switch_active = bool(exists)

    # ---- inbound: orders.new --------------------------------------------

    async def handle_order(self, envelope: dict, _mid: str) -> None:
        """Run the gating checks on one ``orders.new`` envelope."""
        await self.refresh_kill_switch()
        order = dict(envelope.get("data", {}))
        source_owner = envelope.get("owner_id", "unknown")

        # Fail-fast checks in spec order.
        reason = self._check(order)
        if reason is not None:
            await self.publish(
                "orders.rejected",
                {
                    "order_id":        order.get("order_id"),
                    "reason":          reason,
                    "owner_id_source": source_owner,
                    **self._explain(order, reason),
                },
            )
            return

        approved = {
            **order,
            "approved_at":     datetime.now(timezone.utc).isoformat() + "Z",
            "owner_id_source": source_owner,
        }
        await self.publish("orders.approved", approved)

    def _check(self, order: dict) -> str | None:
        instrument = order.get("instrument")
        account    = order.get("account")
        qty        = float(order.get("qty", 0))
        side       = str(order.get("side", "BUY")).upper()

        # 1. Notional
        price = order.get("limit_price") or self.state["prices"].get(instrument)
        if price is None:
            return "no price available"
        notional = abs(qty) * float(price)
        if notional > self.max_notional:
            return "notional limit breached"

        # 2. Position
        signed_qty = qty if side == "BUY" else -qty
        current = float(self.state["positions"].get((account, instrument), 0))
        new_pos = abs(current + signed_qty)
        if new_pos > self.max_position:
            return "position limit breached"

        # 3. Margin (notional * buffer_pct <= available_margin)
        required_margin = notional * self.margin_buffer_pct
        available = float(self.state["margin"].get(account, 0))
        if required_margin > available:
            return "margin insufficient"

        # 4. Volatility
        vol = self.state["volatility"].get(instrument)
        if vol is not None and float(vol) > self.vol_threshold:
            return "volatility above threshold"

        # 5. Kill switch
        if self.kill_switch_active:
            return "kill switch active"

        return None

    def _explain(self, order: dict, reason: str) -> dict[str, Any]:
        """Attach the most relevant numeric context to a rejection."""
        instrument = order.get("instrument")
        account    = order.get("account")
        qty        = float(order.get("qty", 0))
        price      = order.get("limit_price") or self.state["prices"].get(instrument)
        if reason == "notional limit breached":
            return {"limit": self.max_notional,
                    "actual": abs(qty) * float(price or 0)}
        if reason == "position limit breached":
            current = float(self.state["positions"].get((account, instrument), 0))
            side = str(order.get("side", "BUY")).upper()
            signed = qty if side == "BUY" else -qty
            return {"limit": self.max_position,
                    "actual": abs(current + signed)}
        if reason == "margin insufficient":
            return {"required": abs(qty) * float(price or 0) * self.margin_buffer_pct,
                    "available": float(self.state["margin"].get(account, 0))}
        if reason == "volatility above threshold":
            return {"limit": self.vol_threshold,
                    "actual": self.state["volatility"].get(instrument)}
        return {}

    @staticmethod
    def _mid_price(bid, ask) -> float | None:
        if bid is None or ask is None:
            return None
        try:
            return (float(bid) + float(ask)) / 2.0
        except (TypeError, ValueError):
            return None

    # ---- rejection-spike tracking ---------------------------------------

    async def track_rejection(self, envelope: dict, _mid: str) -> None:
        """Subscribed to ``orders.rejected``; counts per source owner_id."""
        d = envelope.get("data", {})
        source_owner = d.get("owner_id_source") or envelope.get("owner_id")
        if not source_owner:
            return
        now = time.time()
        window = self._rejections.setdefault(source_owner, deque())
        window.append(now)
        cutoff = now - self.rejection_window_s
        while window and window[0] < cutoff:
            window.popleft()
        if len(window) >= self.rejection_threshold:
            # Throttle: at most one alert per window.
            last = self._last_spike_alert_at.get(source_owner, 0)
            if now - last >= self.rejection_window_s:
                await self.publish(
                    "health.risk.rejection_spike",
                    {
                        "owner_id":   source_owner,
                        "count":      len(window),
                        "window_s":   self.rejection_window_s,
                        "threshold":  self.rejection_threshold,
                    },
                )
                self._last_spike_alert_at[source_owner] = now

    # ---- main run loop --------------------------------------------------

    async def run(self) -> None:  # pragma: no cover
        await self.subscribe("orders.new",        self.handle_order)
        await self.subscribe("market.price.*",    self.handle_market_price)
        await self.subscribe("market.vol.*",      self.handle_market_vol)
        await self.subscribe("positions.update",  self.handle_position_update)
        await self.subscribe("account.margin",    self.handle_margin_update)
        await self.subscribe("orders.rejected",   self.track_rejection)
        await self.on_start()
        await asyncio.Event().wait()


def main() -> None:  # pragma: no cover
    import logging as _logging
    from core.redis_bus import RedisBus

    _logging.basicConfig(level=_logging.INFO)

    async def _entry() -> None:
        bus = RedisBus()
        await bus.connect()
        try:
            await RiskGate(bus=bus).run()
        finally:
            await bus.disconnect()

    asyncio.run(_entry())


if __name__ == "__main__":  # pragma: no cover
    main()
