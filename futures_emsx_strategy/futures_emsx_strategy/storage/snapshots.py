"""Periodic snapshots of internal state, written as JSON blobs to the `snapshots` table."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .db import Database


class SnapshotStore:
    def __init__(self, db: Database) -> None:
        self.db = db

    def save(self, name: str, payload: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO snapshots (name, captured_at, payload_json) VALUES (?, ?, ?)",
            (name, datetime.now(timezone.utc).isoformat(), json.dumps(payload)),
        )

    def latest(self, name: str) -> dict[str, Any] | None:
        rows = self.db.query(
            "SELECT payload_json FROM snapshots WHERE name = ? "
            "ORDER BY captured_at DESC LIMIT 1",
            (name,),
        )
        return json.loads(rows[0]["payload_json"]) if rows else None
