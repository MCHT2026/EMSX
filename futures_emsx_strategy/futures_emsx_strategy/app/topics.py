"""Canonical topic names + per-topic event type registry.

Registering the dataclass for each topic at import time allows the serializing
buses (Kafka, Redis) to reconstruct typed events when consuming. Single-process
buses ignore the registry. The call is idempotent and safe to import twice.
"""
from ..core.events import (
    BarClosed,
    ExecutionUpdate,
    FillUpdate,
    KillSwitchEvent,
    MarketTick,
    OrderIntent,
    RiskDecision,
    TargetPosition,
)
from ..messaging.codec import register_topic

TICKS = "ticks"
BARS = "bars"
TARGETS = "targets"
INTENTS = "intents"
RISK_DECISIONS = "risk_decisions"
EXECUTION_UPDATES = "execution_updates"
FILLS = "fills"
KILL_SWITCH = "kill_switch"


def register_topic_types() -> None:
    """Bind canonical event dataclasses to their topics. Idempotent."""
    register_topic(TICKS, MarketTick)
    register_topic(BARS, BarClosed)
    register_topic(TARGETS, TargetPosition)
    register_topic(INTENTS, OrderIntent)
    register_topic(RISK_DECISIONS, RiskDecision)
    register_topic(EXECUTION_UPDATES, ExecutionUpdate)
    register_topic(FILLS, FillUpdate)
    register_topic(KILL_SWITCH, KillSwitchEvent)


register_topic_types()
