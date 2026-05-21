"""Typed repositories for state tables (positions, working_orders, fills, risk_decisions)."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from ..core.events import FillUpdate, RiskDecision
from ..orders.lifecycle import OrderRecord
from .db import Database


class PositionRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def upsert(self, instrument: str, qty: int, avg_cost: float) -> None:
        self.db.execute(
            "INSERT INTO positions (instrument, qty, avg_cost, updated_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(instrument) DO UPDATE SET qty=excluded.qty, "
            "avg_cost=excluded.avg_cost, updated_at=excluded.updated_at",
            (instrument, qty, avg_cost, datetime.now(timezone.utc).isoformat()),
        )

    def get(self, instrument: str) -> tuple[int, float] | None:
        rows = self.db.query("SELECT qty, avg_cost FROM positions WHERE instrument=?", (instrument,))
        if not rows:
            return None
        r = rows[0]
        return int(r["qty"]), float(r["avg_cost"])

    def all(self) -> dict[str, tuple[int, float]]:
        rows = self.db.query("SELECT instrument, qty, avg_cost FROM positions")
        return {r["instrument"]: (int(r["qty"]), float(r["avg_cost"])) for r in rows}


class OrderRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def upsert(self, rec: OrderRecord, leaves_qty: int) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.db.execute(
            """INSERT INTO working_orders
                (order_id, venue_order_id, route_id, strategy_id, instrument,
                 side, qty, leaves_qty, filled_qty, avg_price, status,
                 idempotency_key, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(order_id) DO UPDATE SET
                venue_order_id=excluded.venue_order_id,
                route_id=excluded.route_id,
                leaves_qty=excluded.leaves_qty,
                filled_qty=excluded.filled_qty,
                avg_price=excluded.avg_price,
                status=excluded.status,
                updated_at=excluded.updated_at""",
            (
                rec.order_id,
                rec.venue_order_id,
                rec.route_id,
                rec.strategy_id,
                rec.instrument,
                rec.side.value,
                rec.qty,
                leaves_qty,
                rec.filled_qty,
                rec.avg_price,
                rec.status.value,
                rec.idempotency_key,
                rec.created_at.isoformat(),
                now,
            ),
        )

    def by_idempotency_key(self, key: str) -> dict | None:
        rows = self.db.query(
            "SELECT * FROM working_orders WHERE idempotency_key = ?", (key,)
        )
        return dict(rows[0]) if rows else None


class FillRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def insert(self, fill: FillUpdate) -> None:
        self.db.execute(
            "INSERT INTO fills (order_id, route_id, instrument, side, "
            "fill_qty, fill_price, occurred_at) VALUES (?,?,?,?,?,?,?)",
            (
                fill.order_id,
                fill.route_id,
                fill.instrument,
                fill.side.value,
                fill.fill_qty,
                fill.fill_price,
                fill.timestamp.isoformat(),
            ),
        )

    def for_order(self, order_id: str) -> list[dict]:
        rows = self.db.query("SELECT * FROM fills WHERE order_id=? ORDER BY id", (order_id,))
        return [dict(r) for r in rows]


class RiskDecisionRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def insert(self, decision: RiskDecision) -> None:
        self.db.execute(
            "INSERT INTO risk_decisions (order_key, approved, reasons_json, decided_at) "
            "VALUES (?,?,?,?)",
            (
                decision.order_key,
                1 if decision.approved else 0,
                json.dumps(list(decision.reasons)),
                decision.decided_at.isoformat(),
            ),
        )
