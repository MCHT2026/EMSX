"""Tracks idempotency keys we have already submitted.

Backends must provide an *atomic* ``claim(key) -> bool`` that returns True iff
this caller is the one that inserted the row. Check-then-add is not enough
under concurrent processes; SQLite's ``INSERT OR IGNORE`` plus
``cursor.rowcount`` is the canonical pattern.
"""
from __future__ import annotations

from threading import Lock
from typing import Protocol


class IdempotencyBackend(Protocol):
    def has(self, key: str) -> bool: ...
    def add(self, key: str) -> None: ...
    def claim(self, key: str) -> bool: ...


class _InMemoryBackend:
    def __init__(self) -> None:
        self._seen: set[str] = set()
        self._lock = Lock()

    def has(self, key: str) -> bool:
        with self._lock:
            return key in self._seen

    def add(self, key: str) -> None:
        with self._lock:
            self._seen.add(key)

    def claim(self, key: str) -> bool:
        with self._lock:
            if key in self._seen:
                return False
            self._seen.add(key)
            return True


class IdempotencyStore:
    def __init__(self, backend: IdempotencyBackend | None = None) -> None:
        self._backend = backend or _InMemoryBackend()

    def seen(self, key: str) -> bool:
        return self._backend.has(key)

    def mark(self, key: str) -> None:
        self._backend.add(key)

    def claim(self, key: str) -> bool:
        """Atomic claim. Returns True iff this call is the one that inserted
        the row; concurrent callers racing on the same key see at most one
        ``True``. Backwards-compatible fallback for legacy backends without a
        native ``claim`` is provided but is *not* race-safe."""
        backend_claim = getattr(self._backend, "claim", None)
        if backend_claim is not None:
            return bool(backend_claim(key))
        if self._backend.has(key):
            return False
        self._backend.add(key)
        return True
