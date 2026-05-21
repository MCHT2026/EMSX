"""SQLite-backed idempotency backend. Restarts cannot resubmit duplicate orders.

``claim`` is atomic: it uses ``INSERT OR IGNORE`` plus ``cursor.rowcount`` so
that two concurrent processes racing on the same key see exactly one ``True``.
"""
from __future__ import annotations

from datetime import datetime, timezone

from .db import Database


class SqliteIdempotencyBackend:
    """Persistent dedupe over ``submitted_intents.idempotency_key`` (PRIMARY KEY).

    Conforms to the ``IdempotencyBackend`` protocol expected by
    :class:`futures_emsx_strategy.orders.idempotency.IdempotencyStore`.
    """

    def __init__(self, db: Database) -> None:
        self.db = db

    def has(self, key: str) -> bool:
        rows = self.db.query(
            "SELECT 1 FROM submitted_intents WHERE idempotency_key = ?", (key,)
        )
        return bool(rows)

    def add(self, key: str) -> None:
        # Kept for the IdempotencyBackend protocol. Non-racy callers should
        # prefer ``claim`` so they can act on whether the row was actually
        # inserted by this call.
        self.claim(key)

    def claim(self, key: str) -> bool:
        """Atomic insert. Returns True iff this call actually inserted the row.

        ``INSERT OR IGNORE`` is a single statement; SQLite serializes writes,
        and ``rowcount`` reports 1 on insert / 0 on the ignored conflict.
        """
        ts = datetime.now(timezone.utc).isoformat()
        with self.db.cursor() as cur:
            cur.execute(
                "INSERT OR IGNORE INTO submitted_intents "
                "(idempotency_key, submitted_at) VALUES (?, ?)",
                (key, ts),
            )
            return cur.rowcount == 1
