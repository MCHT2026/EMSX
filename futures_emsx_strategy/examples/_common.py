"""Helpers shared across the examples."""
from __future__ import annotations

import signal
import threading
from pathlib import Path

from futures_emsx_strategy.config.loader import AppConfig, load_app_config
from futures_emsx_strategy.core.logging import configure_logging, get_logger

DEFAULT_CONFIG_DIR = str(Path(__file__).resolve().parents[1] / "config")


def setup(level: str = "INFO") -> None:
    configure_logging(level=level, json=False)


def load_config(config_dir: str | None) -> AppConfig:
    return load_app_config(config_dir or DEFAULT_CONFIG_DIR)


def install_signal_handler() -> threading.Event:
    """Returns an Event that is set on SIGINT or SIGTERM. Examples loop on it."""
    stop = threading.Event()

    def _handler(*_a) -> None:
        stop.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    return stop


def banner(title: str, safety: str) -> None:
    log = get_logger("example")
    log.info("example_start", title=title, safety=safety)
    print(f"\n=== {title}  [{safety}]\n")


def require_blpapi() -> None:
    """Soft-import check. Examples print a clear message if the user forgot to
    install the bloomberg extra."""
    try:
        import blpapi  # noqa: F401
    except ImportError as e:
        raise SystemExit(
            "blpapi is not installed. From the repo root run:\n"
            "  pip install -e \".[bloomberg]\"\n"
            f"Original error: {e}"
        ) from None
