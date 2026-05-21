-- Schema for event log + state tables. SQLite-compatible.
-- Designed to be portable to Postgres with minor changes (SERIAL -> INTEGER, etc.).

CREATE TABLE IF NOT EXISTS event_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at     TEXT    NOT NULL,
    event_type      TEXT    NOT NULL,
    correlation_id  TEXT,
    payload_json    TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_event_log_type     ON event_log(event_type);
CREATE INDEX IF NOT EXISTS ix_event_log_corr     ON event_log(correlation_id);
CREATE INDEX IF NOT EXISTS ix_event_log_occurred ON event_log(occurred_at);

CREATE TABLE IF NOT EXISTS positions (
    instrument      TEXT PRIMARY KEY,
    qty             INTEGER NOT NULL,
    avg_cost        REAL    NOT NULL,
    updated_at      TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS working_orders (
    order_id        TEXT PRIMARY KEY,
    venue_order_id  TEXT,
    route_id        TEXT,
    strategy_id     TEXT NOT NULL,
    instrument      TEXT NOT NULL,
    side            TEXT NOT NULL,
    qty             INTEGER NOT NULL,
    leaves_qty      INTEGER NOT NULL,
    filled_qty      INTEGER NOT NULL,
    avg_price       REAL,
    status          TEXT NOT NULL,
    idempotency_key TEXT UNIQUE NOT NULL,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_working_orders_instr ON working_orders(instrument);
CREATE INDEX IF NOT EXISTS ix_working_orders_strat ON working_orders(strategy_id);
CREATE INDEX IF NOT EXISTS ix_working_orders_status ON working_orders(status);

CREATE TABLE IF NOT EXISTS fills (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        TEXT NOT NULL,
    route_id        TEXT,
    instrument      TEXT NOT NULL,
    side            TEXT NOT NULL,
    fill_qty        INTEGER NOT NULL,
    fill_price      REAL    NOT NULL,
    occurred_at     TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_fills_order ON fills(order_id);

CREATE TABLE IF NOT EXISTS risk_decisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    order_key       TEXT NOT NULL,
    approved        INTEGER NOT NULL,
    reasons_json    TEXT    NOT NULL,
    decided_at      TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_risk_order_key ON risk_decisions(order_key);

CREATE TABLE IF NOT EXISTS minute_bars (
    instrument      TEXT NOT NULL,
    start_time      TEXT NOT NULL,
    end_time        TEXT NOT NULL,
    open            REAL NOT NULL,
    high            REAL NOT NULL,
    low             REAL NOT NULL,
    close           REAL NOT NULL,
    volume          INTEGER NOT NULL,
    interval_min    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (instrument, start_time, interval_min)
);

CREATE TABLE IF NOT EXISTS snapshots (
    name            TEXT NOT NULL,
    captured_at     TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    PRIMARY KEY (name, captured_at)
);

CREATE TABLE IF NOT EXISTS kill_switch_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tripped         INTEGER NOT NULL,
    reason          TEXT NOT NULL,
    actor           TEXT NOT NULL,
    occurred_at     TEXT NOT NULL
);
