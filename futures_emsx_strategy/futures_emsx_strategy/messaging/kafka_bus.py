"""Kafka-backed bus (confluent-kafka). Soft-imported so dev environments don't need it."""
from __future__ import annotations

import json
import threading
from dataclasses import asdict, is_dataclass
from typing import Any

from ..core.logging import get_logger
from .bus import EventBus, Handler

log = get_logger(__name__)

try:
    from confluent_kafka import Consumer, Producer
    _HAVE_KAFKA = True
except ImportError:
    Consumer = None  # type: ignore[misc,assignment]
    Producer = None  # type: ignore[misc,assignment]
    _HAVE_KAFKA = False


def _to_json(o: Any) -> str:
    if is_dataclass(o):
        o = asdict(o)
    return json.dumps(o, default=str)


class KafkaBus(EventBus):
    def __init__(self, bootstrap: str, group_id: str = "fes") -> None:
        if not _HAVE_KAFKA:
            raise RuntimeError("confluent-kafka not installed; pip install futures_emsx_strategy[kafka]")
        self.bootstrap = bootstrap
        self.group_id = group_id
        self._producer = Producer({"bootstrap.servers": bootstrap})
        self._subs: dict[str, list[Handler]] = {}
        self._consumer_thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._subs:
            self._consumer_thread = threading.Thread(target=self._consume, daemon=True)
            self._consumer_thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._consumer_thread is not None:
            self._consumer_thread.join(timeout=5.0)
        self._producer.flush(5.0)

    def publish(self, topic: str, payload: Any) -> None:
        self._producer.produce(topic, _to_json(payload).encode("utf-8"))
        self._producer.poll(0)

    def subscribe(self, topic: str, handler: Handler) -> None:
        self._subs.setdefault(topic, []).append(handler)

    def _consume(self) -> None:
        c = Consumer({
            "bootstrap.servers": self.bootstrap,
            "group.id": self.group_id,
            "auto.offset.reset": "latest",
        })
        c.subscribe(list(self._subs.keys()))
        while not self._stop.is_set():
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                payload = json.loads(msg.value())
            except json.JSONDecodeError:
                log.warning("kafka_bad_message", topic=msg.topic())
                continue
            for h in self._subs.get(msg.topic(), []):
                try:
                    h(msg.topic(), payload)
                except Exception:  # noqa: BLE001
                    log.exception("kafka_handler_failed", topic=msg.topic())
        c.close()
