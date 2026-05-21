"""OrderManager: turns TargetPosition events into OrderIntent events.

Decision rule:
    delta = target_qty - current_position - net_working_qty
If delta is zero, no order. Else side = sign(delta), qty = abs(delta).

Idempotency: each intent carries a deterministic ``idempotency_key``. The
manager *checks* (via ``IdempotencyStore.seen``) whether the key was already
committed and suppresses regeneration in that case --- but it does NOT claim
the key here. The runner claims atomically *after* risk approval, just before
submitting to the venue. That way a transient risk rejection (e.g. stale
data) leaves the key un-claimed so the next bar / retry can succeed.
"""
from __future__ import annotations

from ..core.enums import OrderType, Side, TimeInForce
from ..core.events import OrderIntent, TargetPosition
from ..core.identifiers import idempotency_key
from ..core.logging import get_logger
from .idempotency import IdempotencyStore
from .models import WorkingOrderBook
from .sizing import slice_order

log = get_logger(__name__)


class OrderManager:
    def __init__(
        self,
        positions,
        working: WorkingOrderBook,
        idempotency: IdempotencyStore,
        default_order_type: OrderType = OrderType.MKT,
        default_tif: TimeInForce = TimeInForce.DAY,
        max_clip: int | None = None,
    ) -> None:
        self.positions = positions
        self.working = working
        self.idempotency = idempotency
        self.default_order_type = default_order_type
        self.default_tif = default_tif
        self.max_clip = max_clip

    def on_target(self, target: TargetPosition) -> list[OrderIntent]:
        current = self.positions.position(target.instrument)
        working = self.working.net_working_qty(target.instrument)
        delta = target.target_qty - current - working

        if delta == 0:
            log.debug(
                "target_no_change",
                instrument=target.instrument,
                target=target.target_qty,
                current=current,
                working=working,
            )
            return []

        side = Side.from_delta(delta)
        total_qty = abs(delta)
        clips = slice_order(total_qty, self.max_clip) if self.max_clip else [total_qty]

        intents: list[OrderIntent] = []
        for i, clip in enumerate(clips):
            key = idempotency_key(
                strategy_id=target.strategy_id,
                instrument=target.instrument,
                bar_end=target.timestamp,
                side=side.value,
                qty=clip if len(clips) == 1 else (clip * 1000 + i),
            )
            if self.idempotency.seen(key):
                # Already committed by a previous successful submission; do
                # not regenerate. Suppression by ``seen`` is a read-only check
                # so transient risk rejections do NOT poison this key.
                log.info(
                    "order_intent_suppressed_already_submitted",
                    key=key,
                    strategy=target.strategy_id,
                    instrument=target.instrument,
                )
                continue
            intent = OrderIntent(
                strategy_id=target.strategy_id,
                instrument=target.instrument,
                side=side,
                qty=clip,
                order_type=self.default_order_type,
                time_in_force=self.default_tif,
                idempotency_key=key,
                source_timestamp=target.timestamp,
                metadata={
                    "reason": target.reason,
                    "current_position": current,
                    "working": working,
                    "target": target.target_qty,
                    "delta": delta,
                },
            )
            intents.append(intent)
            log.info(
                "order_intent",
                key=key,
                strategy=target.strategy_id,
                instrument=target.instrument,
                side=side.value,
                qty=clip,
                current=current,
                working=working,
                target=target.target_qty,
            )
        return intents
