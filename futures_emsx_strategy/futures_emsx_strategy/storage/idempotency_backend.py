"""SQLite-backed idempotency backend. Restarts cannot resubmit duplicate orders."""
from __future__ import annotations

import sqlite3
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
        try:
            self.db.execute(
                "INSERT INTO submitted_intents (idempotency_key, submitted_at) "
                "VALUES (?, ?)",
                (key, datetime.now(timezone.utc).isoformat()),
            )
        except sqlite3.IntegrityError:
            # Already present (concurrent claim) -- safe to ignore.
            pass
