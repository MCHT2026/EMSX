#!/usr/bin/env bash
# Run the live-market-data example and stream ticks to stdout.
#
# Usage:
#   examples/run_market_data.sh                          # all instruments, 30s
#   examples/run_market_data.sh -i "ESM6 Index"          # single instrument
#   examples/run_market_data.sh -s 0                     # run until Ctrl+C
#   examples/run_market_data.sh -s 120 -l ticks.log      # tee to a log
#   examples/run_market_data.sh --bars                   # switch to minute bars
#
# Environment overrides:
#   PYTHON      python executable (default: python3 if present, else python)
#   CONFIG_DIR  path to config/ directory (default: <repo>/config)

set -euo pipefail

# --- resolve paths ----------------------------------------------------------
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

# --- defaults ---------------------------------------------------------------
PYTHON="${PYTHON:-$(command -v python3 || command -v python || true)}"
CONFIG_DIR="${CONFIG_DIR:-$REPO_ROOT/config}"
INSTRUMENT=""
SECONDS_ARG=30
MODE="ticks"          # ticks | bars
LOG_FILE=""

usage() {
  sed -n '2,12p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

# --- arg parse --------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    -i|--instrument)  INSTRUMENT="$2"; shift 2 ;;
    -s|--seconds)     SECONDS_ARG="$2"; shift 2 ;;
    -c|--config-dir)  CONFIG_DIR="$2"; shift 2 ;;
    -l|--log)         LOG_FILE="$2"; shift 2 ;;
    --bars)           MODE="bars"; shift ;;
    --ticks)          MODE="ticks"; shift ;;
    -h|--help)        usage 0 ;;
    *) echo "Unknown argument: $1" >&2; usage 1 ;;
  esac
done

# --- preflight --------------------------------------------------------------
if [[ -z "$PYTHON" ]]; then
  echo "ERROR: no python executable on PATH (set PYTHON=/path/to/python to override)" >&2
  exit 1
fi
if [[ ! -d "$CONFIG_DIR" ]]; then
  echo "ERROR: config dir not found: $CONFIG_DIR" >&2
  exit 1
fi
if ! "$PYTHON" -c "import blpapi" >/dev/null 2>&1; then
  echo "ERROR: blpapi is not importable in $PYTHON" >&2
  echo "  Install with:  $PYTHON -m pip install -e \"$REPO_ROOT[bloomberg]\"" >&2
  exit 1
fi

# --- pick module + args -----------------------------------------------------
case "$MODE" in
  ticks) MODULE="examples.02_live_ticks" ;;
  bars)  MODULE="examples.03_minute_bars" ;;
  *)     echo "Unknown mode: $MODE" >&2; exit 1 ;;
esac

CMD=( "$PYTHON" -m "$MODULE" --config-dir "$CONFIG_DIR" --seconds "$SECONDS_ARG" )
if [[ -n "$INSTRUMENT" ]]; then
  CMD+=( --instrument "$INSTRUMENT" )
fi

# --- banner -----------------------------------------------------------------
echo "==========================================================="
echo " Live $MODE  (Ctrl+C to stop)"
echo "   repo root  : $REPO_ROOT"
echo "   config     : $CONFIG_DIR"
echo "   python     : $PYTHON"
echo "   instrument : ${INSTRUMENT:-<all from instruments.yaml>}"
echo "   duration   : ${SECONDS_ARG}s  (0 = until SIGINT)"
[[ -n "$LOG_FILE" ]] && echo "   log file   : $LOG_FILE"
echo "==========================================================="
echo

# --- run --------------------------------------------------------------------
cd "$REPO_ROOT"
trap 'echo; echo "stopped."' INT TERM

if [[ -n "$LOG_FILE" ]]; then
  "${CMD[@]}" 2>&1 | tee "$LOG_FILE"
else
  "${CMD[@]}"
fi
