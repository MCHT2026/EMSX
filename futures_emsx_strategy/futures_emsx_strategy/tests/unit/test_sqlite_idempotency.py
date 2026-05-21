"""Regression: idempotency claims must survive process restart.

Mirrors the failure mode where a re-played bar after a crash would re-claim
the same key (in-memory store had no record of it) and resubmit a duplicate
order.
"""
from __future__ import annotations

from futures_emsx_strategy.orders.idempotency import IdempotencyStore
from futures_emsx_strategy.storage.db import Database
from futures_emsx_strategy.storage.idempotency_backend import SqliteIdempotencyBackend


def _make_store(db_path: str) -> tuple[Database, IdempotencyStore]:
    db = Database(f"sqlite:///{db_path}")
    return db, IdempotencyStore(backend=SqliteIdempotencyBackend(db))


def test_claim_survives_new_instance(tmp_path):
    db_path = tmp_path / "fes.db"
    _, store = _make_store(str(db_path))
    assert store.claim("minute_es_v1:abc123") is True
    # Simulate a process restart: build a fresh Store on the same file.
    db2, store2 = _make_store(str(db_path))
    assert store2.seen("minute_es_v1:abc123") is True
    assert store2.claim("minute_es_v1:abc123") is False
    db2.close()


def test_unrelated_keys_are_independent(tmp_path):
    db_path = tmp_path / "fes.db"
    _, store = _make_store(str(db_path))
    assert store.claim("k1") is True
    assert store.claim("k2") is True
    assert store.claim("k1") is False
    assert store.claim("k2") is False
