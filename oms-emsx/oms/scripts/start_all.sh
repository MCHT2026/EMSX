#!/usr/bin/env bash
#
# start_all.sh — launch the OMS EMSX stack in the order required by the spec.
#
# Order:
#   1. Redis primary       (port 6379)
#   2. Redis replica       (port 6380, replicaof 127.0.0.1 6379)
#   3. Redis Sentinel      (port 26379, config/sentinel.conf)
#   4. Archiver            (must be up before any other publisher)
#   5. Watchdog
#   6. Risk gate
#   7. EMSX gateway
#   8. User strategy modules (anything in modules/strategy_*.py)
#
# Each process writes its PID to pids/{name}.pid. stop_all.sh reads these.
#
# Health gating: after each Redis step we ping; after each OMS process we
# wait up to 5s for its PID file to appear and verify the process is alive.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PID_DIR="$ROOT/pids"
LOG_DIR="$ROOT/logs"
mkdir -p "$PID_DIR" "$LOG_DIR"

PY="${PYTHON:-python}"

write_pid() {
    local name="$1" pid="$2"
    echo "$pid" > "$PID_DIR/${name}.pid"
}

is_running() {
    local pid="$1"
    kill -0 "$pid" 2>/dev/null
}

wait_for_redis() {
    local port="$1" timeout=15
    for _ in $(seq "$timeout"); do
        if redis-cli -p "$port" ping >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    echo "redis on port $port failed to come up" >&2
    return 1
}

start_redis_primary() {
    echo ">> starting redis primary on 6379"
    redis-server --port 6379 --daemonize yes \
        --pidfile "$PID_DIR/redis_primary.pid" \
        --logfile "$LOG_DIR/redis_primary.log"
    wait_for_redis 6379
}

start_redis_replica() {
    echo ">> starting redis replica on 6380"
    redis-server --port 6380 --replicaof 127.0.0.1 6379 --daemonize yes \
        --pidfile "$PID_DIR/redis_replica.pid" \
        --logfile "$LOG_DIR/redis_replica.log"
    wait_for_redis 6380
}

start_sentinel() {
    echo ">> starting redis-sentinel on 26379"
    redis-sentinel "$ROOT/config/sentinel.conf" --daemonize yes \
        --pidfile "$PID_DIR/redis_sentinel.pid" \
        --logfile "$LOG_DIR/redis_sentinel.log"
    sleep 1
}

start_module() {
    local name="$1" script="$2"
    echo ">> starting $name"
    nohup "$PY" "$script" >"$LOG_DIR/${name}.log" 2>&1 &
    local pid=$!
    write_pid "$name" "$pid"
    sleep 1
    if ! is_running "$pid"; then
        echo "$name failed to start — see $LOG_DIR/${name}.log" >&2
        return 1
    fi
}

start_redis_primary
start_redis_replica
start_sentinel

# Archiver MUST be running before any other publisher so the WAL captures
# every message from t=0.
start_module archiver     modules/archiver.py
start_module watchdog     modules/watchdog.py
start_module risk_gate    modules/risk_gate.py
start_module emsx_gateway modules/emsx_gateway.py

# User strategy modules (optional). Add to modules/strategy_*.py and they
# will be auto-started here.
for strat in modules/strategy_*.py; do
    [ -f "$strat" ] || continue
    name="$(basename "$strat" .py)"
    start_module "$name" "$strat"
done

echo
echo "all services running. logs in $LOG_DIR/. pids in $PID_DIR/."
