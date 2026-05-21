"""Topic <-> dataclass serialization for buses that cross a process boundary.

Handlers expect typed events (e.g. ``BarClosed``), so any bus that serializes
to JSON (Kafka, Redis) must decode the payload back to the registered class
before invoking handlers. In-process buses (``InMemoryBus``) ignore the codec
and pass the original object reference.

Topic types are registered once at process start via ``app.topics``.
"""
from __future__ import annotations

import dataclasses
import json
import types
import typing
from datetime import datetime
from enum import Enum
from typing import Any

_TOPIC_TO_CLASS: dict[str, type] = {}


def register_topic(topic: str, event_type: type) -> None:
    """Idempotent registration: same topic + same class is allowed."""
    existing = _TOPIC_TO_CLASS.get(topic)
    if existing is not None and existing is not event_type:
        raise ValueError(
            f"Topic {topic!r} already registered to {existing.__name__}, "
            f"refusing to rebind to {event_type.__name__}"
        )
    _TOPIC_TO_CLASS[topic] = event_type


def topic_type(topic: str) -> type | None:
    return _TOPIC_TO_CLASS.get(topic)


def to_jsonable(o: Any) -> Any:
    """Recursively convert dataclasses/enums/datetimes into JSON-safe primitives."""
    if dataclasses.is_dataclass(o) and not isinstance(o, type):
        return {f.name: to_jsonable(getattr(o, f.name)) for f in dataclasses.fields(o)}
    if isinstance(o, dict):
        return {str(k): to_jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [to_jsonable(v) for v in o]
    if isinstance(o, Enum):
        return o.value
    if isinstance(o, datetime):
        return o.isoformat()
    return o


def encode(payload: Any) -> str:
    return json.dumps(to_jsonable(payload), default=str)


def decode(topic: str, raw: Any) -> Any:
    """Reconstruct the registered dataclass for ``topic``. Pass-through if unknown."""
    cls = _TOPIC_TO_CLASS.get(topic)
    if cls is None or not isinstance(raw, dict):
        return raw
    return from_dict(cls, raw)


def from_dict(cls: type, data: dict) -> Any:
    hints = typing.get_type_hints(cls)
    kwargs: dict[str, Any] = {}
    for f in dataclasses.fields(cls):
        if f.name in data:
            kwargs[f.name] = _coerce(data[f.name], hints.get(f.name))
    return cls(**kwargs)


def _coerce(value: Any, type_: Any) -> Any:
    if value is None or type_ is None:
        return value

    origin = typing.get_origin(type_)
    if origin is typing.Union or origin is types.UnionType:
        for arg in typing.get_args(type_):
            if arg is type(None):
                continue
            try:
                return _coerce(value, arg)
            except (ValueError, TypeError):
                continue
        return value

    if origin in (list, tuple):
        args = typing.get_args(type_)
        inner = args[0] if args else None
        return [_coerce(v, inner) for v in value]

    if origin is dict:
        return value if isinstance(value, dict) else {}

    if dataclasses.is_dataclass(type_):
        return from_dict(type_, value) if isinstance(value, dict) else value

    if isinstance(type_, type):
        if issubclass(type_, Enum):
            return type_(value)
        if issubclass(type_, datetime):
            return datetime.fromisoformat(value) if isinstance(value, str) else value

    return value
