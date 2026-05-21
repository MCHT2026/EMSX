"""Regression: SqliteIdempotencyBackend.claim must be atomic across processes."""
from __future__ import annotations

import threading

from futures_emsx_strategy.orders.idempotency import IdempotencyStore
from futures_emsx_strategy.storage.db import Database
from futures_emsx_strategy.storage.idempotency_backend import SqliteIdempotencyBackend


def _store(path: str) -> IdempotencyStore:
    return IdempotencyStore(SqliteIdempotencyBackend(Database(f"sqlite:///{path}")))


def test_concurrent_claim_winners_unique(tmp_path):
    path = tmp_path / "fes.db"
    A = _store(str(path))
    B = _store(str(path))
    results: dict[str, list[bool]] = {"A": [], "B": []}
    barrier = threading.Barrier(2)

    def race(name: str, store: IdempotencyStore, keys: list[str]) -> None:
        barrier.wait()
        for k in keys:
            results[name].append(store.claim(k))

    keys = [f"K{i}" for i in range(200)]
    tA = threading.Thread(target=race, args=("A", A, keys))
    tB = threading.Thread(target=race, args=("B", B, keys))
    tA.start(); tB.start(); tA.join(); tB.join()

    # For every key, exactly one of (A,B) sees True.
    for i, k in enumerate(keys):
        winners = int(results["A"][i]) + int(results["B"][i])
        assert winners == 1, f"key {k}: winners={winners}"


def test_inmemory_claim_is_thread_safe():
    store = IdempotencyStore()  # default in-memory backend
    barrier = threading.Barrier(8)
    winners: list[bool] = []
    lock = threading.Lock()

    def attempt() -> None:
        barrier.wait()
        won = store.claim("K")
        with lock:
            winners.append(won)

    threads = [threading.Thread(target=attempt) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(winners) == 1
