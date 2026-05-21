"""Tracks idempotency keys we have already submitted. Survives process restart if backed by storage."""
from __future__ import annotations

from threading import Lock
from typing import Protocol


class IdempotencyBackend(Protocol):
    def has(self, key: str) -> bool: ...
    def add(self, key: str) -> None: ...


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


class IdempotencyStore:
    def __init__(self, backend: IdempotencyBackend | None = None) -> None:
        self._backend = backend or _InMemoryBackend()

    def seen(self, key: str) -> bool:
        return self._backend.has(key)

    def mark(self, key: str) -> None:
        self._backend.add(key)

    def claim(self, key: str) -> bool:
        """Atomic-ish: returns True if this key is new (and now marked)."""
        if self._backend.has(key):
            return False
        self._backend.add(key)
        return True
