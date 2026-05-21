"""Append-only event log. Two backends: JSONL file and SQLite event_log table."""
from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from threading import Lock
from typing import Any

from .db import Database


def _to_jsonable(o: Any) -> Any:
    if is_dataclass(o):
        return _to_jsonable(asdict(o))
    if isinstance(o, dict):
        return {k: _to_jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_to_jsonable(v) for v in o]
    if isinstance(o, Enum):
        return o.value
    if isinstance(o, datetime):
        return o.isoformat()
    return o


class EventLog(ABC):
    @abstractmethod
    def append(self, event_type: str, event: Any, correlation_id: str | None = None) -> None: ...


class JsonlEventLog(EventLog):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()

    def append(self, event_type: str, event: Any, correlation_id: str | None = None) -> None:
        record = {
            "occurred_at": datetime.utcnow().isoformat() + "Z",
            "event_type": event_type,
            "correlation_id": correlation_id,
            "payload": _to_jsonable(event),
        }
        line = json.dumps(record, separators=(",", ":"))
        with self._lock, self.path.open("a") as f:
            f.write(line + "\n")


class SqliteEventLog(EventLog):
    def __init__(self, db: Database) -> None:
        self.db = db

    def append(self, event_type: str, event: Any, correlation_id: str | None = None) -> None:
        payload = json.dumps(_to_jsonable(event), separators=(",", ":"))
        self.db.execute(
            "INSERT INTO event_log (occurred_at, event_type, correlation_id, payload_json) "
            "VALUES (?, ?, ?, ?)",
            (datetime.utcnow().isoformat() + "Z", event_type, correlation_id, payload),
        )
