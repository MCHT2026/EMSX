"""Redis Streams bus. Soft-imported.

Payloads round-trip through ``codec.encode/decode`` so handlers always receive
the registered dataclass type, not a raw dict.
"""
from __future__ import annotations

import json
import threading
from typing import Any

from ..core.logging import get_logger
from .bus import EventBus, Handler
from .codec import decode, encode

log = get_logger(__name__)

try:
    import redis
    _HAVE_REDIS = True
except ImportError:
    redis = None  # type: ignore[assignment]
    _HAVE_REDIS = False


class RedisStreamBus(EventBus):
    def __init__(self, url: str, group: str = "fes", consumer: str = "fes-1") -> None:
        if not _HAVE_REDIS:
            raise RuntimeError("redis not installed; pip install futures_emsx_strategy[redis]")
        self.url = url
        self.group = group
        self.consumer = consumer
        self._r = redis.Redis.from_url(url, decode_responses=True)
        self._subs: dict[str, list[Handler]] = {}
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        for topic in self._subs:
            try:
                self._r.xgroup_create(topic, self.group, id="$", mkstream=True)
            except redis.ResponseError:
                pass
        if self._subs:
            self._thread = threading.Thread(target=self._consume, daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def publish(self, topic: str, payload: Any) -> None:
        self._r.xadd(topic, {"data": encode(payload)})

    def subscribe(self, topic: str, handler: Handler) -> None:
        self._subs.setdefault(topic, []).append(handler)

    def _consume(self) -> None:
        streams = {topic: ">" for topic in self._subs}
        while not self._stop.is_set():
            resp = self._r.xreadgroup(self.group, self.consumer, streams, count=64, block=1000)
            if not resp:
                continue
            for topic, messages in resp:
                for msg_id, data in messages:
                    try:
                        raw = json.loads(data["data"])
                    except (json.JSONDecodeError, KeyError):
                        log.warning("redis_bad_message", topic=topic)
                        continue
                    typed = decode(topic, raw)
                    for h in self._subs.get(topic, []):
                        try:
                            h(topic, typed)
                        except Exception:  # noqa: BLE001
                            log.exception("redis_handler_failed", topic=topic)
                    self._r.xack(topic, self.group, msg_id)
