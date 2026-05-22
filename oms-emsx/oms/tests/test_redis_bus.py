"""Tests for RedisBus — the Redis-Streams implementation of EventBus.

Uses ``fakeredis.aioredis`` so the test suite has no external dependency
on a live Redis instance. Verifies:

- publish serialises the envelope to a single ``data`` field and returns the
  Redis stream id
- consume_once delivers via XREADGROUP, dispatches to the handler, and acks
- get_pending returns the PEL after a consumer reads without acking
- replay_pending claims and redelivers the PEL on startup
- wildcard subscriptions match all known topics in the registry
- the outbound buffer drains on reconnect (Redis transiently unavailable)
- set_nx returns True on first call, False on the second
"""
from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from fakeredis import aioredis as fake_aioredis

from core.redis_bus import RedisBus, TOPIC_REGISTRY_KEY


@pytest.fixture
async def bus():
    fake = fake_aioredis.FakeRedis(decode_responses=False)
    b = RedisBus(redis_client=fake)
    await b.connect()
    yield b
    await b.disconnect()


# ---- publish + consume_once ----------------------------------------------

async def test_publish_returns_stream_id_and_stores_data(bus: RedisBus):
    envelope = {
        "owner_id": "strategy",
        "message_id": "abc",
        "timestamp": "2026-05-22T12:00:00.000000Z",
        "topic": "orders.new",
        "data": {"order_id": "o1"},
    }
    msg_id = await bus.publish("orders.new", envelope)
    assert msg_id  # non-empty stream id

    # Stored as a single JSON field
    raw = await bus.redis.xrange("orders.new")
    assert len(raw) == 1
    _, fields = raw[0]
    stored = json.loads(fields[b"data"])
    assert stored == envelope


async def test_publish_registers_topic(bus: RedisBus):
    envelope = {"owner_id": "s", "message_id": "1", "timestamp": "t",
                "topic": "orders.new", "data": {}}
    await bus.publish("orders.new", envelope)
    members = await bus.redis.smembers(TOPIC_REGISTRY_KEY)
    assert b"orders.new" in members


async def test_consume_once_delivers_and_acks(bus: RedisBus):
    envelope = {"owner_id": "s", "message_id": "abc", "timestamp": "t",
                "topic": "orders.new", "data": {"k": 1}}
    await bus.publish("orders.new", envelope)

    received: list[tuple[dict, str]] = []

    async def handler(env: dict, mid: str) -> None:
        received.append((env, mid))
        # Caller is responsible for ack — simulate that here.
        await bus.ack("orders.new", "risk_gate", mid)

    await bus.ensure_group("orders.new", "risk_gate")
    await bus.consume_once("orders.new", "risk_gate", "risk_gate:host1", handler)

    assert len(received) == 1
    assert received[0][0] == envelope

    # PEL should now be empty.
    pending = await bus.get_pending("orders.new", "risk_gate")
    assert pending == []


async def test_get_pending_returns_unacked(bus: RedisBus):
    envelope = {"owner_id": "s", "message_id": "abc", "timestamp": "t",
                "topic": "orders.new", "data": {}}
    await bus.publish("orders.new", envelope)
    await bus.ensure_group("orders.new", "risk_gate")

    # Handler that does NOT ack.
    async def handler(env: dict, mid: str) -> None:
        pass

    await bus.consume_once("orders.new", "risk_gate", "risk_gate:host1", handler)

    pending = await bus.get_pending("orders.new", "risk_gate")
    assert len(pending) == 1


async def test_replay_pending_redelivers(bus: RedisBus):
    envelope = {"owner_id": "s", "message_id": "abc", "timestamp": "t",
                "topic": "orders.new", "data": {"k": 1}}
    await bus.publish("orders.new", envelope)
    await bus.ensure_group("orders.new", "risk_gate")

    # First consumer reads but does NOT ack.
    async def silent(env: dict, mid: str) -> None:
        pass

    await bus.consume_once("orders.new", "risk_gate", "risk_gate:host1", silent)
    pending_before = await bus.get_pending("orders.new", "risk_gate")
    assert len(pending_before) == 1

    # Replay -> handler is called with the original envelope.
    seen: list[dict] = []

    async def replayer(env: dict, mid: str) -> None:
        seen.append(env)
        await bus.ack("orders.new", "risk_gate", mid)

    await bus.replay_pending("orders.new", "risk_gate", "risk_gate:host1", replayer)
    assert len(seen) == 1
    assert seen[0] == envelope

    pending_after = await bus.get_pending("orders.new", "risk_gate")
    assert pending_after == []


# ---- wildcard subscription -----------------------------------------------

async def test_wildcard_matches_all_known_topics(bus: RedisBus):
    # Pre-register some topics.
    e = lambda topic: {"owner_id": "s", "message_id": "x", "timestamp": "t",
                        "topic": topic, "data": {}}
    await bus.publish("fills.partial", e("fills.partial"))
    await bus.publish("fills.done",    e("fills.done"))
    await bus.publish("orders.new",    e("orders.new"))

    matches = await bus.resolve_topics("fills.*")
    assert set(matches) == {"fills.partial", "fills.done"}

    all_topics = await bus.resolve_topics("*")
    assert {"fills.partial", "fills.done", "orders.new"} <= set(all_topics)


# ---- outbound buffer (Redis transiently unavailable) ---------------------

async def test_outbound_buffer_drains_on_reconnect(bus: RedisBus):
    # Mark bus as disconnected; publishes go to the buffer.
    bus._connected = False
    env = {"owner_id": "s", "message_id": "1", "timestamp": "t",
           "topic": "orders.new", "data": {"i": 1}}
    msg_id = await bus.publish("orders.new", env)
    assert msg_id == ""  # buffered, no real stream id yet
    assert bus._buffer.qsize() == 1

    # Reconnect drains the buffer.
    bus._connected = True
    await bus.drain_buffer()
    assert bus._buffer.qsize() == 0
    raw = await bus.redis.xrange("orders.new")
    assert len(raw) == 1


# ---- idempotency helper ---------------------------------------------------

async def test_set_nx_first_call_succeeds_second_fails(bus: RedisBus):
    assert await bus.set_nx("processed:m:abc", ttl_s=10) is True
    assert await bus.set_nx("processed:m:abc", ttl_s=10) is False
