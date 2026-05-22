"""Tests for the EventBus abstract interface.

The interface itself is abstract — these tests verify the contract:
the required methods exist with the right signatures, and instantiation
of the bare ABC fails.
"""
from __future__ import annotations

import inspect

import pytest

from core.event_bus import EventBus


def test_eventbus_is_abstract():
    """EventBus is an ABC; constructing it directly must fail."""
    with pytest.raises(TypeError):
        EventBus()  # type: ignore[abstract]


@pytest.mark.parametrize(
    "method_name",
    [
        "publish",
        "subscribe",
        "ack",
        "get_pending",
        "replay_pending",
        "connect",
        "disconnect",
    ],
)
def test_eventbus_declares_method(method_name: str):
    """Each required method is declared on EventBus."""
    assert hasattr(EventBus, method_name), f"EventBus missing {method_name}"
    method = getattr(EventBus, method_name)
    assert callable(method)


def test_eventbus_methods_are_coroutines():
    """All EventBus methods are async (return coroutines)."""
    for name in ("publish", "subscribe", "ack", "get_pending",
                 "replay_pending", "connect", "disconnect"):
        method = getattr(EventBus, name)
        assert inspect.iscoroutinefunction(method), f"{name} must be async"


def test_publish_signature():
    """publish(topic: str, payload: dict) -> str."""
    sig = inspect.signature(EventBus.publish)
    params = list(sig.parameters)
    assert params == ["self", "topic", "payload"]


def test_subscribe_signature():
    """subscribe(topic_pattern, group, consumer, handler)."""
    sig = inspect.signature(EventBus.subscribe)
    params = list(sig.parameters)
    assert params == ["self", "topic_pattern", "group", "consumer", "handler"]
