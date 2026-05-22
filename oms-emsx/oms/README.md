# OMS EMSX

Modular Order Management System for mid-frequency futures trading. Bloomberg EMSX is the execution venue.

## Architecture

Each component is a separate OS process. All communication goes through a Redis Streams event bus. No component talks to another component directly.

```
strategy -> orders.new -> risk_gate -> orders.approved -> emsx_gateway -> Bloomberg
                                                                     <- fills.partial / fills.done
                                                                        ^
                                                                        |
                                                          all topics -> archiver (WAL)
                                                          health.*   -> watchdog
```

## Components

- `core/base_module.py` — `BaseModule` base class: publish/subscribe/ack/heartbeat/idempotency
- `core/event_bus.py` — `EventBus` abstract interface
- `core/redis_bus.py` — Redis Streams implementation (with Sentinel HA)
- `modules/risk_gate.py` — mandatory pre-trade pipeline stage
- `modules/emsx_gateway.py` — only process that holds a BLPAPI handle
- `modules/archiver.py` — receive-only, writes JSONL WAL + SQLite index
- `modules/watchdog.py` — health monitor, restart manager, Sentinel watcher

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
# bloomberg extra (only needed on a Terminal/B-PIPE host)
pip install -e ".[dev,bloomberg]"
```

## Running

Start Redis (primary + replica + sentinel), then OMS processes:

```bash
./scripts/start_all.sh
```

Stop:

```bash
./scripts/stop_all.sh
```

## Testing

```powershell
pytest -v
```

`tests/mocks/mock_blpapi.py` provides a full BLPAPI session mock so unit tests
do not need a real Terminal connection.
