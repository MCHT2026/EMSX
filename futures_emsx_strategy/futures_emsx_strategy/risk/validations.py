"""Reusable check primitives. Each ValidationCheck is a small dataclass with pass/fail + reason."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ValidationCheck:
    name: str
    passed: bool
    reason: str = ""

    @classmethod
    def ok(cls, name: str) -> "ValidationCheck":
        return cls(name=name, passed=True, reason="")

    @classmethod
    def fail(cls, name: str, reason: str) -> "ValidationCheck":
        return cls(name=name, passed=False, reason=reason)
