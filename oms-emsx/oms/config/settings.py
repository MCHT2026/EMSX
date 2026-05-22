"""Central configuration. All values are overridable via environment variables.

Env var naming convention: the constant name itself, uppercase.
e.g. `HEARTBEAT_INTERVAL_S=2 python -m modules.risk_gate`.
"""
from __future__ import annotations

import os
from typing import Any


def _env(name: str, default: Any, cast: type | None = None) -> Any:
    raw = os.environ.get(name)
    if raw is None:
        return default
    if cast is bool:
        return raw.strip().lower() in {"1", "true", "yes", "on"}
    if cast is None:
        return raw
    return cast(raw)


def _env_sentinel_hosts(default: list[tuple[str, int]]) -> list[tuple[str, int]]:
    raw = os.environ.get("REDIS_SENTINEL_HOSTS")
    if not raw:
        return default
    out: list[tuple[str, int]] = []
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair:
            continue
        host, _, port = pair.partition(":")
        out.append((host, int(port or 26379)))
    return out


# ---- Redis / event bus ----------------------------------------------------

REDIS_SENTINEL_HOSTS: list[tuple[str, int]] = _env_sentinel_hosts([("localhost", 26379)])
REDIS_SENTINEL_MASTER: str = _env("REDIS_SENTINEL_MASTER", "mymaster")
REDIS_PASSWORD: str | None = _env("REDIS_PASSWORD", None)
REDIS_DB: int = _env("REDIS_DB", 0, int)

# Direct (non-Sentinel) URL, used by tests and local dev when no Sentinel is up.
# When set, the bus connects directly instead of going through Sentinel discovery.
REDIS_DIRECT_URL: str | None = _env("REDIS_DIRECT_URL", None)

# Reconnect backoff (milliseconds)
RECONNECT_BACKOFF_INITIAL_MS: int = _env("RECONNECT_BACKOFF_INITIAL_MS", 100, int)
RECONNECT_BACKOFF_CAP_MS: int = _env("RECONNECT_BACKOFF_CAP_MS", 10_000, int)

# XREADGROUP tuning
XREAD_COUNT: int = _env("XREAD_COUNT", 10, int)
XREAD_BLOCK_MS: int = _env("XREAD_BLOCK_MS", 100, int)


# ---- BLPAPI ---------------------------------------------------------------

BLPAPI_HOST: str = _env("BLPAPI_HOST", "localhost")
BLPAPI_PORT: int = _env("BLPAPI_PORT", 8194, int)
BLPAPI_SERVICE_EMSX: str = _env("BLPAPI_SERVICE_EMSX", "//blp/emapisvc")
BLPAPI_HEARTBEAT_INTERVAL_S: int = _env("BLPAPI_HEARTBEAT_INTERVAL_S", 10, int)


# ---- Heartbeats / watchdog -----------------------------------------------

HEARTBEAT_INTERVAL_S: int = _env("HEARTBEAT_INTERVAL_S", 5, int)
WATCHDOG_CHECK_INTERVAL_S: int = _env("WATCHDOG_CHECK_INTERVAL_S", 2, int)
HEARTBEAT_TIMEOUT_S: int = _env("HEARTBEAT_TIMEOUT_S", 10, int)
WATCHDOG_DEAD_TIMEOUT_S: int = _env("WATCHDOG_DEAD_TIMEOUT_S", 30, int)
WATCHDOG_MAX_RESTARTS: int = _env("WATCHDOG_MAX_RESTARTS", 3, int)
WATCHDOG_PEL_CHECK_INTERVAL_S: int = _env("WATCHDOG_PEL_CHECK_INTERVAL_S", 30, int)


# ---- Delivery semantics --------------------------------------------------

IDEMPOTENCY_TTL_S: int = _env("IDEMPOTENCY_TTL_S", 900, int)  # 15 min
PEL_ALERT_THRESHOLD: int = _env("PEL_ALERT_THRESHOLD", 100, int)
LOCAL_BUFFER_MAXSIZE: int = _env("LOCAL_BUFFER_MAXSIZE", 1000, int)


# ---- Risk gate -----------------------------------------------------------

MAX_NOTIONAL: float = _env("MAX_NOTIONAL", 1_000_000.0, float)
MAX_POSITION: float = _env("MAX_POSITION", 100.0, float)
MARGIN_BUFFER_PCT: float = _env("MARGIN_BUFFER_PCT", 0.10, float)
VOL_THRESHOLD: float = _env("VOL_THRESHOLD", 0.05, float)

# Rejection-spike alerting (rolling window in seconds, threshold = count of rejections).
RISK_REJECTION_WINDOW_S: int = _env("RISK_REJECTION_WINDOW_S", 60, int)
RISK_REJECTION_THRESHOLD: int = _env("RISK_REJECTION_THRESHOLD", 10, int)


# ---- Archiver ------------------------------------------------------------

ARCHIVE_LOG_DIR: str = _env("ARCHIVE_LOG_DIR", "logs")
ARCHIVE_DB_PATH: str = _env("ARCHIVE_DB_PATH", "logs/archive_index.sqlite")
ARCHIVE_FLUSH_INTERVAL_S: float = _env("ARCHIVE_FLUSH_INTERVAL_S", 1.0, float)


# ---- Process management ---------------------------------------------------

PID_DIR: str = _env("PID_DIR", "pids")
MODULE_DIR: str = _env("MODULE_DIR", "modules")


# Modules known to the watchdog (name -> entry-point script relative to repo root).
# Append additional user modules here.
KNOWN_MODULES: dict[str, str] = {
    "archiver": "modules/archiver.py",
    "watchdog": "modules/watchdog.py",
    "risk_gate": "modules/risk_gate.py",
    "emsx_gateway": "modules/emsx_gateway.py",
}
