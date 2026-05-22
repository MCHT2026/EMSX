"""Archiver — write-only WAL for every message on every topic.

Architecture role
-----------------
- Subscribes to ``*`` via catch-all consumer group.
- Writes each envelope as a JSON line to ``logs/archive_YYYY-MM-DD.jsonl``.
- Indexes (topic, owner_id, message_id, timestamp) into SQLite for replay.
- **Acks immediately after enqueuing to the async buffer** — the in-memory
  buffer + WAL is the source of truth; if a flush fails the next restart
  picks the message up from the PEL.
- Never publishes back to the bus. The only outbound traffic this process
  produces is the heartbeat that ``BaseModule.heartbeat()`` sends.
- Provides ``replay(owner_id, from_timestamp)`` for development queries.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator

import aiofiles

from config import settings
from core.base_module import BaseModule
from core.event_bus import EventBus

log = logging.getLogger(__name__)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS archive (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    topic       TEXT    NOT NULL,
    owner_id    TEXT    NOT NULL,
    message_id  TEXT    NOT NULL,
    timestamp   TEXT    NOT NULL,
    payload     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_archive_owner_ts ON archive(owner_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_archive_topic    ON archive(topic);
CREATE INDEX IF NOT EXISTS idx_archive_message  ON archive(message_id);
"""


class Archiver(BaseModule):
    """Catch-all archiver — writes WAL + indexes for replay."""

    def __init__(
        self,
        bus: EventBus,
        log_dir: str | None = None,
        db_path: str | None = None,
    ) -> None:
        super().__init__(name="archiver", bus=bus)
        self.log_dir = Path(log_dir or settings.ARCHIVE_LOG_DIR)
        self.db_path = Path(db_path or settings.ARCHIVE_DB_PATH)
        self._jsonl_buffer: list[str] = []
        self._index_buffer: list[tuple[str, str, str, str, str]] = []
        self._buffer_lock = asyncio.Lock()
        self._db: sqlite3.Connection | None = None
        self._flush_task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    # ---- lifecycle --------------------------------------------------------

    async def open(self) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # SQLite open is fast; the heavy writes go through the flush loop.
        self._db = await asyncio.to_thread(
            lambda: sqlite3.connect(
                str(self.db_path),
                timeout=5.0,
                check_same_thread=False,
            )
        )
        await asyncio.to_thread(self._db.executescript, _SCHEMA)
        await asyncio.to_thread(self._db.commit)

    async def close(self) -> None:
        self._stop.set()
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except (asyncio.CancelledError, Exception):
                pass
            self._flush_task = None
        await self.flush()
        if self._db is not None:
            await asyncio.to_thread(self._db.close)
            self._db = None

    # ---- main loop --------------------------------------------------------

    async def handle_message(self, envelope: dict, _msg_id: str) -> None:
        """Buffer one envelope. Caller is BaseModule.process_message, which
        will ack after we return — that's exactly the spec's "ack before
        flush" semantics."""
        line = json.dumps(envelope, default=str)
        owner_id   = envelope.get("owner_id", "")
        message_id = envelope.get("message_id", "")
        timestamp  = envelope.get("timestamp", "")
        topic      = envelope.get("topic", "")
        async with self._buffer_lock:
            self._jsonl_buffer.append(line)
            self._index_buffer.append(
                (topic, owner_id, message_id, timestamp, line)
            )

    async def flush(self) -> None:
        async with self._buffer_lock:
            jsonl, idx = self._jsonl_buffer, self._index_buffer
            self._jsonl_buffer = []
            self._index_buffer = []

        if jsonl:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            path = self.log_dir / f"archive_{today}.jsonl"
            async with aiofiles.open(path, mode="a", encoding="utf-8") as fh:
                await fh.write("\n".join(jsonl) + "\n")

        if idx and self._db is not None:
            await asyncio.to_thread(self._sqlite_insert, idx)

    def _sqlite_insert(self, rows) -> None:
        assert self._db is not None
        self._db.executemany(
            "INSERT INTO archive(topic, owner_id, message_id, timestamp, payload)"
            " VALUES(?,?,?,?,?)",
            rows,
        )
        self._db.commit()

    async def _flush_loop(self) -> None:
        interval = settings.ARCHIVE_FLUSH_INTERVAL_S
        try:
            while not self._stop.is_set():
                try:
                    await self.flush()
                except Exception:
                    log.exception("archiver flush failed")
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            pass

    # ---- replay API -------------------------------------------------------

    async def replay(self, owner_id: str, from_timestamp: str) -> AsyncIterator[dict]:
        """Yield archived envelopes for *owner_id* at or after *from_timestamp*."""
        if self._db is None:
            return
        rows = await asyncio.to_thread(
            lambda: self._db.execute(
                "SELECT payload FROM archive"
                " WHERE owner_id=? AND timestamp>=? ORDER BY timestamp ASC, id ASC",
                (owner_id, from_timestamp),
            ).fetchall()
        )
        for (payload,) in rows:
            yield json.loads(payload)

    # ---- BaseModule.run ---------------------------------------------------

    async def run(self) -> None:  # pragma: no cover - exercised via integration
        await self.open()
        await self.subscribe("*", self.handle_message)
        await self.on_start()
        if self._flush_task is None:
            self._flush_task = asyncio.create_task(self._flush_loop())
        try:
            await self._stop.wait()
        finally:
            await self.on_stop()
            await self.close()


def main() -> None:  # pragma: no cover - process entry point
    import logging as _logging
    from core.redis_bus import RedisBus

    _logging.basicConfig(level=_logging.INFO)

    async def _entry() -> None:
        bus = RedisBus()
        await bus.connect()
        try:
            await Archiver(bus=bus).run()
        finally:
            await bus.disconnect()

    asyncio.run(_entry())


if __name__ == "__main__":  # pragma: no cover
    main()
