#!/usr/bin/env bash
#
# stop_all.sh — graceful shutdown of the OMS EMSX stack.
#
# Stops processes in REVERSE order of start_all.sh:
#   strategies -> emsx_gateway -> risk_gate -> watchdog -> archiver -> sentinel -> replica -> primary
#
# SIGTERM first; if still alive after 5s, SIGKILL.

set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PID_DIR="$ROOT/pids"

stop_pidfile() {
    local name="$1" pidfile="$PID_DIR/${name}.pid"
    if [ ! -f "$pidfile" ]; then
        return 0
    fi
    local pid; pid="$(cat "$pidfile")"
    if ! kill -0 "$pid" 2>/dev/null; then
        rm -f "$pidfile"
        return 0
    fi
    echo ">> SIGTERM $name ($pid)"
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 5); do
        if ! kill -0 "$pid" 2>/dev/null; then
            rm -f "$pidfile"
            return 0
        fi
        sleep 1
    done
    echo ">> SIGKILL $name ($pid)"
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$pidfile"
}

# Reverse start order. Strategies first.
for f in "$PID_DIR"/strategy_*.pid; do
    [ -f "$f" ] || continue
    stop_pidfile "$(basename "$f" .pid)"
done

stop_pidfile emsx_gateway
stop_pidfile risk_gate
stop_pidfile watchdog
stop_pidfile archiver
stop_pidfile redis_sentinel
stop_pidfile redis_replica
stop_pidfile redis_primary

echo "shutdown complete."
