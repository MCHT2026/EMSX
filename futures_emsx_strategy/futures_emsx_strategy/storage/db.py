"""Database wrapper around sqlite3 (with a hook for SQLAlchemy if needed later)."""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from typing import Iterator

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


class Database:
    def __init__(self, url: str) -> None:
        if not url.startswith("sqlite:///"):
            raise ValueError(f"Only sqlite:// URLs supported in this minimal impl: {url}")
        self.path = url.replace("sqlite:///", "", 1)
        self._lock = Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with _SCHEMA_PATH.open("r") as f:
            sql = f.read()
        with self._lock:
            self._conn.executescript(sql)

    @contextmanager
    def cursor(self) -> Iterator[sqlite3.Cursor]:
        with self._lock:
            cur = self._conn.cursor()
            try:
                yield cur
            finally:
                cur.close()

    def execute(self, sql: str, params: tuple = ()) -> None:
        with self.cursor() as cur:
            cur.execute(sql, params)

    def executemany(self, sql: str, params_list: list[tuple]) -> None:
        with self.cursor() as cur:
            cur.executemany(sql, params_list)

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def close(self) -> None:
        with self._lock:
            self._conn.close()


_DB: Database | None = None


def get_database(url: str | None = None) -> Database:
    global _DB
    if _DB is None:
        if url is None:
            raise RuntimeError("Database not initialized; pass url on first call")
        _DB = Database(url)
    return _DB
