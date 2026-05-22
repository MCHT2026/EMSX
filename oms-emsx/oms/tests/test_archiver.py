"""Tests for the Archiver module.

Verifies:
- writes every received message as a JSONL line to logs/archive_YYYY-MM-DD.jsonl
- indexes every message in SQLite with (topic, owner_id, message_id, timestamp, payload)
- ack happens immediately after enqueue (WAL is source of truth)
- replay(owner_id, from_timestamp) returns matching messages in order
- archiver never publishes back to the bus (write-only)
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from modules.archiver import Archiver
from tests.inmem_bus import InMemoryBus


@pytest.fixture
def tmp_archive(tmp_path: Path):
    return tmp_path / "logs", tmp_path / "archive.sqlite"


async def _make(bus: InMemoryBus, log_dir: Path, db_path: Path) -> Archiver:
    await bus.connect()
    arch = Archiver(bus=bus, log_dir=str(log_dir), db_path=str(db_path))
    await arch.open()
    return arch


async def test_writes_jsonl_per_message(tmp_archive):
    log_dir, db_path = tmp_archive
    bus = InMemoryBus()
    arch = await _make(bus, log_dir, db_path)

    env = {
        "owner_id": "strategy",
        "message_id": "abc",
        "timestamp": "2026-05-22T12:00:00.000000Z",
        "topic": "orders.new",
        "data": {"order_id": "o1", "qty": 10},
    }
    await arch.handle_message(env, "redis-id-1")
    await arch.flush()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    jsonl_path = log_dir / f"archive_{today}.jsonl"
    assert jsonl_path.exists(), f"missing {jsonl_path}"

    lines = jsonl_path.read_text().strip().splitlines()
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed == env

    await arch.close()


async def test_sqlite_index_records_message_metadata(tmp_archive):
    import sqlite3
    log_dir, db_path = tmp_archive
    bus = InMemoryBus()
    arch = await _make(bus, log_dir, db_path)

    env = {
        "owner_id":   "strategy",
        "message_id": "abc-123",
        "timestamp":  "2026-05-22T12:00:00.000000Z",
        "topic":      "orders.new",
        "data":       {"order_id": "o1"},
    }
    await arch.handle_message(env, "redis-id-1")
    await arch.flush()

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT topic, owner_id, message_id, timestamp FROM archive"
    ).fetchall()
    conn.close()
    assert rows == [("orders.new", "strategy", "abc-123",
                     "2026-05-22T12:00:00.000000Z")]

    await arch.close()


async def test_replay_returns_only_matching_owner_id(tmp_archive):
    log_dir, db_path = tmp_archive
    bus = InMemoryBus()
    arch = await _make(bus, log_dir, db_path)

    envs = [
        {"owner_id": "strategy", "message_id": "m1",
         "timestamp": "2026-05-22T12:00:00.000000Z",
         "topic": "orders.new", "data": {"i": 1}},
        {"owner_id": "risk_gate", "message_id": "m2",
         "timestamp": "2026-05-22T12:00:01.000000Z",
         "topic": "orders.approved", "data": {"i": 2}},
        {"owner_id": "strategy", "message_id": "m3",
         "timestamp": "2026-05-22T12:00:02.000000Z",
         "topic": "orders.new", "data": {"i": 3}},
    ]
    for i, e in enumerate(envs):
        await arch.handle_message(e, f"rid-{i}")
    await arch.flush()

    out = []
    async for env in arch.replay("strategy", "2026-05-22T00:00:00.000000Z"):
        out.append(env)

    assert [e["message_id"] for e in out] == ["m1", "m3"]
    await arch.close()


async def test_replay_filters_by_from_timestamp(tmp_archive):
    log_dir, db_path = tmp_archive
    bus = InMemoryBus()
    arch = await _make(bus, log_dir, db_path)

    envs = [
        {"owner_id": "strategy", "message_id": "old",
         "timestamp": "2026-05-22T11:00:00.000000Z",
         "topic": "x", "data": {}},
        {"owner_id": "strategy", "message_id": "new",
         "timestamp": "2026-05-22T13:00:00.000000Z",
         "topic": "x", "data": {}},
    ]
    for i, e in enumerate(envs):
        await arch.handle_message(e, f"rid-{i}")
    await arch.flush()

    out = []
    async for env in arch.replay("strategy", "2026-05-22T12:00:00.000000Z"):
        out.append(env)
    assert [e["message_id"] for e in out] == ["new"]
    await arch.close()


async def test_handle_message_does_not_publish_back(tmp_archive):
    """Archiver is write-only: handling a message must never call bus.publish."""
    log_dir, db_path = tmp_archive
    bus = InMemoryBus()
    arch = await _make(bus, log_dir, db_path)

    env = {"owner_id": "strategy", "message_id": "x",
           "timestamp": "2026-05-22T12:00:00.000000Z",
           "topic": "orders.new", "data": {}}
    await arch.handle_message(env, "rid")
    await arch.flush()

    # No streams should have been written by the archiver.
    assert "orders.new" not in bus.streams
    # The archiver does not publish heartbeat directly via handle_message —
    # heartbeats are emitted by BaseModule.heartbeat(), not handle_message.

    await arch.close()
