"""Stable ID generators used as idempotency keys and correlation IDs."""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime


def new_uuid() -> str:
    return str(uuid.uuid4())


def correlation_id(prefix: str = "corr") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def idempotency_key(
    strategy_id: str,
    instrument: str,
    bar_end: datetime,
    side: str,
    qty: int,
) -> str:
    """Deterministic order key: same inputs always produce the same key.

    Used by OrderManager to suppress duplicates across restarts and replays.
    """
    raw = f"{strategy_id}|{instrument}|{bar_end.isoformat()}|{side}|{qty}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return f"{strategy_id}:{digest}"


def emsx_request_id(order_id: str, action: str) -> str:
    return f"{action}:{order_id}:{uuid.uuid4().hex[:8]}"
