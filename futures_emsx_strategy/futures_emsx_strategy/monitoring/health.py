"""Health aggregator. Each subsystem registers a probe; HealthChecker rolls them up."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass
class HealthProbe:
    name: str
    check: Callable[[], bool]
    detail: Callable[[], str] = lambda: ""


class HealthChecker:
    def __init__(self) -> None:
        self._probes: list[HealthProbe] = []

    def register(self, probe: HealthProbe) -> None:
        self._probes.append(probe)

    def check_all(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for p in self._probes:
            try:
                ok = bool(p.check())
                out[p.name] = {"ok": ok, "detail": p.detail()}
            except Exception as e:  # noqa: BLE001
                out[p.name] = {"ok": False, "detail": f"probe_error: {e!r}"}
        return out

    def is_healthy(self) -> bool:
        return all(v["ok"] for v in self.check_all().values())
