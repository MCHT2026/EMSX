# OMS EMSX — Project Specification

## Overview

Build a modular, resilient **Order Management System (OMS)** for **mid-frequency futures trading**, using Bloomberg EMSX as the execution venue. The system is designed as a **slow path** execution layer (as opposed to low-latency direct market access), targeting order latencies in the tens to hundreds of milliseconds range, which is well within tolerance for the intended use case.

Each component runs as a **separate OS process**. All inter-process communication happens exclusively through a **central event bus backed by Redis Streams**. No component ever communicates directly with another component — every message, in every direction, goes through the bus.

---

## Technology Stack

- **Language:** Python 3.11+
- **Async runtime:** `asyncio` within each process
- **Message bus:** Redis Streams via `aioredis`
- **Redis HA:** Redis Sentinel (primary + replica + sentinel process)
- **Bloomberg connectivity:** `blpapi` (Bloomberg API Python SDK)
- **Dependency management:** `poetry` or `pip` with `requirements.txt`
- **Testing:** `pytest` + `pytest-asyncio`, with a mock BLPAPI layer for unit tests

---

## Architecture Principles

1. **Every component is a separate OS process.** No shared memory between components.
2. **The bus is the only integration point.** Components publish and subscribe to topics. They never call each other directly.
3. **Every component inherits from `BaseModule`.** This provides owner ID, publish, subscribe, heartbeat, ack, and idempotency handling.
4. **The bus preserves `owner_id` on every message.** No message ever loses its origin identity.
5. **The risk gate is a mandatory pipeline stage**, not a peer module. No order reaches Bloomberg without passing through it.
6. **Blocking calls (BLPAPI, disk I/O) run in an executor.** Never block the asyncio event loop.
7. **At-least-once delivery via Redis PEL.** Every module explicitly `XACK`s after processing. Unacked messages are replayed on restart.
8. **Idempotency via Redis `SET NX` with 15-minute TTL.** Every module checks before processing. Key format: `processed:{module_id}:{message_id}`.

---

## Project Structure

```
oms/
├── core/
│   ├── base_module.py        # BaseModule abstract class
│   ├── event_bus.py          # EventBus abstract interface
│   └── redis_bus.py          # Redis Streams implementation
├── modules/
│   ├── risk_gate.py          # Mandatory pipeline stage
│   ├── emsx_gateway.py       # Bloomberg BLPAPI gateway
│   ├── archiver.py           # Write-ahead log, receive-only
│   ├── watchdog.py           # Health monitor + restart
│   └── module_base.py        # Placeholder for user modules
├── config/
│   ├── settings.py           # Central config (Redis URL, Sentinel, BLPAPI)
│   └── sentinel.conf         # Redis Sentinel configuration
├── tests/
│   ├── test_base_module.py
│   ├── test_event_bus.py
│   ├── test_risk_gate.py
│   ├── test_emsx_gateway.py
│   ├── test_archiver.py
│   ├── test_watchdog.py
│   └── mocks/
│       └── mock_blpapi.py    # Full mock of BLPAPI session
├── scripts/
│   ├── start_all.sh          # Start all processes
│   └── stop_all.sh           # Stop all processes
├── pyproject.toml
└── README.md
```

---

## Core: BaseModule

Every component inherits from `BaseModule`. It provides:

```python
class BaseModule(ABC):
    name: str                  # unique owner ID e.g. "strategy", "mod_1"
    bus: EventBus

    async def publish(topic: str, payload: dict)
    # Automatically stamps {"owner_id": self.name, "message_id": uuid4, "timestamp": utcnow} on every message

    async def subscribe(topic_pattern: str, handler: Callable)
    # Supports wildcard patterns e.g. "fills.*", "orders.*", "health.*"

    async def ack(message)
    # Sends XACK to Redis Streams for the given message

    async def is_duplicate(message_id: str) -> bool
    # Redis SET NX check: key = f"processed:{self.name}:{message_id}", TTL = 900s (15 min)

    async def process_message(message)
    # Wraps handler: check duplicate → SET NX → handle → ack. If duplicate, ack and skip.

    async def heartbeat()
    # Publishes {"owner_id": self.name} to "health.heartbeat" every 5 seconds

    async def on_start()
    # Called on startup. Replays pending PEL messages before consuming new ones.

    async def on_stop()
    # Graceful shutdown hook.

    @abstractmethod
    async def run()
    # Main loop for the module.
```

Key detail: `publish` always injects `owner_id`, `message_id` (UUID4), and `timestamp` (UTC ISO8601). These fields are **never optional** and **never overridable** by the caller.

---

## Core: EventBus Interface

```python
class EventBus(ABC):
    async def publish(topic: str, payload: dict) -> str
    # Returns the Redis stream message ID

    async def subscribe(topic_pattern: str, group: str, consumer: str, handler: Callable)
    # Consumer group subscription. Supports glob patterns on topic names.

    async def ack(topic: str, group: str, message_id: str)
    # XACK

    async def get_pending(topic: str, group: str) -> list
    # Returns PEL (pending entries list) for a consumer group

    async def replay_pending(topic: str, group: str, consumer: str, handler: Callable)
    # Claims and redelivers all pending messages for this consumer on startup

    async def connect()
    async def disconnect()
```

---

## Core: RedisBus Implementation

- Uses `aioredis` with **Sentinel support** for HA
- Sentinel config: 1 primary, 1 replica, 1 sentinel process
- `down-after-milliseconds: 2000` — detect failure within 2 seconds
- `failover-timeout: 5000` — complete failover within 5 seconds
- Each module connects with its own `aioredis` client using Sentinel
- Topics map directly to Redis Stream keys e.g. `orders.new`, `fills.partial`
- Wildcard subscriptions (`fills.*`) are implemented by subscribing to all matching stream keys, resolved at subscribe time from a topic registry
- Consumer group name = module `name` (owner ID)
- Consumer name = `f"{module_name}:{socket.gethostname()}"`
- On publish, the message dict is serialised to JSON and stored as a single Redis stream field `data`
- On consume, `XREADGROUP` with `COUNT=10` and `BLOCK=100` (ms)
- Each module maintains a **local in-memory buffer** (`asyncio.Queue(maxsize=1000)`) for outbound messages during Redis unavailability. On reconnect, the buffer drains in order before new messages are published.
- Reconnect strategy: exponential backoff starting at 100ms, cap at 10s, indefinite retries

---

## Component: Risk Gate

**Process name:** `risk_gate`  
**Owner ID:** `"risk_gate"`

The risk gate is a **mandatory pipeline stage**. It is the only component authorised to publish `orders.approved`. The EMSX gateway only consumes `orders.approved` — it ignores everything else.

**Subscribes to:**
- `orders.new` — orders to evaluate
- `market.price.*` — live price per instrument (published by market data feed or strategy)
- `market.vol.*` — volatility signals per instrument
- `positions.update` — current net/gross position per account and instrument
- `account.margin` — available margin from FCM

**Internal state (held in memory, updated continuously):**
```python
state = {
    "prices": {},       # instrument → latest price
    "volatility": {},   # instrument → latest vol signal
    "positions": {},    # account+instrument → net position
    "margin": {}        # account → available margin
}
```

**On `orders.new`:**
1. Check idempotency (`SET NX`)
2. Run checks in order — fail fast:
   - Notional limit: `qty * price <= max_notional`
   - Position limit: `current_position + qty <= max_position`
   - Margin check: `required_margin <= available_margin`
   - Volatility check: reject if `vol > vol_threshold` (configurable)
   - Kill switch: reject all if kill switch is active
3. If all pass: publish `orders.approved` with full original payload + `approved_at` timestamp
4. If any fail: publish `orders.rejected` with `owner_id` preserved, `reason` field added
5. `XACK` in both cases

**Kill switch:**  
A Redis key `risk_gate:kill_switch` — if it exists, all orders are rejected. The watchdog or an operator can set/delete this key. The risk gate checks it on every order evaluation.

**On `orders.rejected` (its own rejections):**  
The risk gate also subscribes to `orders.rejected` to track rejection rate per `owner_id`. If rejection rate exceeds threshold within a rolling window, it publishes `health.risk.rejection_spike` to alert the watchdog.

---

## Component: EMSX Gateway

**Process name:** `emsx_gateway`  
**Owner ID:** `"emsx_gateway"`

This is the **only component that talks to Bloomberg**. It owns the BLPAPI session entirely. No other component has any knowledge of BLPAPI.

**Subscribes to:**
- `orders.approved` — the only topic it acts on for sending orders

**Publishes:**
- `fills.partial` — partial fill received from EMSX
- `fills.done` — order fully filled
- `orders.cancelled` — order cancelled at EMSX
- `orders.rejected_emsx` — order rejected by Bloomberg/broker (distinct from risk gate rejection)
- `health.emsx.connected` / `health.emsx.disconnected` — session state

**BLPAPI session management:**
- Session options target Bloomberg Terminal at `localhost:8194` by default (configurable for B-PIPE)
- Session runs in a **dedicated thread** via `loop.run_in_executor(None, ...)` — never blocks the asyncio event loop
- On `SessionStarted`: publish `health.emsx.connected`
- On `SessionTerminated`: publish `health.emsx.disconnected`, begin reconnect with exponential backoff
- Heartbeat: ping Bloomberg session every 10 seconds, publish `health.emsx.heartbeat`

**Order subscription:**
- On startup, subscribe to `OrderRouteSubscription` for all active orders and routes
- EMSX subscription events are received in the executor thread and dispatched onto the asyncio event loop via `loop.call_soon_threadsafe`

**Sending an order:**
1. Consume `orders.approved` from bus
2. Check idempotency
3. Translate payload to EMSX request fields:
   - `EMSX_TICKER`, `EMSX_SIDE`, `EMSX_AMOUNT`, `EMSX_ORDER_TYPE`
   - `EMSX_BROKER`, `EMSX_HAND_INSTRUCTION` (from `exec_style` field on the order)
   - Preserve `owner_id` and `message_id` in `EMSX_NOTES` field for traceability
4. Send via `blpapi` in executor
5. `XACK`

**Receiving fills:**
- Parse `EMSX_FILLED`, `EMSX_AVG_PRICE`, `EMSX_STATUS` from subscription events
- Translate to internal fill schema and publish to `fills.*` on the bus
- Fill messages include original `owner_id` (recovered from `EMSX_NOTES`)

**exec_style mapping:**
```python
EXEC_STYLE_MAP = {
    "market":        {"EMSX_HAND_INSTRUCTION": "MKT"},
    "vwap":          {"EMSX_HAND_INSTRUCTION": "VWAP"},
    "twap":          {"EMSX_HAND_INSTRUCTION": "TWAP"},
    "passive_limit": {"EMSX_HAND_INSTRUCTION": "LIMIT"},
}
```

---

## Component: Archiver

**Process name:** `archiver`  
**Owner ID:** `"archiver"`

Write-only. Receives every message on every topic. Never publishes back to the bus (no outbound traffic).

**Subscribes to:** `*` — all topics via a catch-all consumer group

**Behaviour:**
- Writes each message as a JSON line to a daily rotating log file: `logs/archive_YYYY-MM-DD.jsonl`
- Flushes to disk via an async buffer — never blocks on disk I/O (uses `aiofiles`)
- Indexes messages by `owner_id` in a lightweight SQLite database for per-module replay queries
- Schema: `(id, topic, owner_id, message_id, timestamp, payload)`
- `XACK`s immediately after writing to buffer (before flush) — disk flush is best-effort, WAL is the source of truth
- On startup, replays its own PEL before consuming new messages

**Replay API (internal, not on bus):**  
Exposes a simple async method for other components to query during development/testing:
```python
async def replay(owner_id: str, from_timestamp: str) -> AsyncIterator[dict]
```

---

## Component: Watchdog

**Process name:** `watchdog`  
**Owner ID:** `"watchdog"`

Monitors all other processes. Publishes to the bus and can trigger restarts via `subprocess`.

**Subscribes to:**
- `health.heartbeat` — all module heartbeats
- `health.*` — all health events

**Publishes:**
- `health.degraded` — when a module misses N heartbeats
- `health.restored` — when a previously degraded module resumes heartbeating
- `health.restarted` — after attempting a process restart
- `health.bus.failover` — when Redis Sentinel failover is detected

**Tracking:**
```python
registry = {
    "strategy":     {"last_seen": datetime, "status": "alive|degraded|dead", "pid": int},
    "risk_gate":    {...},
    "emsx_gateway": {...},
    "archiver":     {...},
    "mod_1":        {...},
    ...
}
```

**Heartbeat check loop:** runs every 2 seconds  
- If a module's `last_seen` is > 10 seconds ago → publish `health.degraded`, attempt restart  
- If a module's `last_seen` is > 30 seconds ago → publish `health.dead`  
- Restart attempt: `subprocess.Popen([sys.executable, f"modules/{module_name}.py"])`  
- Max 3 restart attempts before publishing `health.dead` and giving up

**PEL monitoring:**  
Every 30 seconds, queries Redis PEL size for each consumer group. If PEL > 100 for any module, publishes `health.pel_growing` with `owner_id` and count — this is an early warning that a module is consuming but not acking.

**Redis Sentinel monitoring:**  
Subscribes to Redis Sentinel pub/sub channel `+switch-master` to detect failovers and publish `health.bus.failover` onto the bus.

---

## Message Schema

Every message on the bus conforms to this envelope. Fields injected automatically by `BaseModule.publish`:

```json
{
  "owner_id":   "strategy",
  "message_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp":  "2024-01-15T09:30:00.123456Z",
  "topic":      "orders.new",
  "data": {
    // topic-specific payload
  }
}
```

### Topic Payloads

**`orders.new`**
```json
{
  "order_id":   "uuid",
  "instrument": "ESH4 Index",
  "side":       "BUY",
  "qty":        10,
  "order_type": "LIMIT",
  "limit_price": 4850.25,
  "exec_style": "vwap",
  "broker":     "GSCO",
  "account":    "ACC001"
}
```

**`orders.approved`** — same as `orders.new` + `approved_at`

**`orders.rejected`**
```json
{
  "order_id": "uuid",
  "reason":   "notional limit breached",
  "limit":    1000000,
  "actual":   1250000
}
```

**`fills.partial` / `fills.done`**
```json
{
  "order_id":    "uuid",
  "fill_id":     "uuid",
  "filled_qty":  5,
  "total_filled": 5,
  "avg_price":   4851.00,
  "timestamp":   "...",
  "broker":      "GSCO"
}
```

**`health.heartbeat`**
```json
{
  "owner_id": "strategy",
  "pid":      12345,
  "status":   "alive"
}
```

**`market.price.{instrument}`**
```json
{
  "instrument": "ESH4 Index",
  "bid":        4850.00,
  "ask":        4850.25,
  "last":       4850.12
}
```

**`positions.update`**
```json
{
  "account":    "ACC001",
  "instrument": "ESH4 Index",
  "net":        10,
  "gross":      10,
  "avg_cost":   4845.00
}
```

---

## Configuration

All configuration lives in `config/settings.py` and is overridable via environment variables:

```python
REDIS_SENTINEL_HOSTS    = [("localhost", 26379)]
REDIS_SENTINEL_MASTER   = "mymaster"
REDIS_PASSWORD          = None

BLPAPI_HOST             = "localhost"
BLPAPI_PORT             = 8194

HEARTBEAT_INTERVAL_S    = 5
WATCHDOG_CHECK_INTERVAL_S = 2
HEARTBEAT_TIMEOUT_S     = 10
WATCHDOG_DEAD_TIMEOUT_S = 30
WATCHDOG_MAX_RESTARTS   = 3

IDEMPOTENCY_TTL_S       = 900   # 15 minutes
PEL_ALERT_THRESHOLD     = 100

LOCAL_BUFFER_MAXSIZE    = 1000  # messages buffered during Redis outage

# Risk gate limits (defaults, overridable per account)
MAX_NOTIONAL            = 1_000_000
MAX_POSITION            = 100
MARGIN_BUFFER_PCT       = 0.10  # keep 10% margin buffer
VOL_THRESHOLD           = 0.05  # 5% vol spike triggers rejection
```

---

## Redis Sentinel Setup

```
# sentinel.conf
sentinel monitor mymaster 127.0.0.1 6379 1
sentinel down-after-milliseconds mymaster 2000
sentinel failover-timeout mymaster 5000
sentinel parallel-syncs mymaster 1
```

Start order:
1. `redis-server --port 6379`  (primary)
2. `redis-server --port 6380 --replicaof 127.0.0.1 6379`  (replica)
3. `redis-sentinel sentinel.conf`

---

## Testing Strategy

**Unit tests** — mock the bus entirely. Each module is tested in isolation.

**Integration tests** — use a real local Redis instance. Test the full message flow from publish to consume to ack.

**BLPAPI mock** — `tests/mocks/mock_blpapi.py` must implement the full session interface:
- `SessionOptions`, `Session`, `SessionStarted`, `SessionTerminated`
- `OrderSubscription`, `RouteSubscription`
- Synthetic fill events on a configurable delay

**Key test cases to cover:**
- Duplicate message is acked and skipped without processing
- PEL messages are replayed on module restart before new messages
- Risk gate rejects correctly on each check type
- `owner_id` is preserved end-to-end from `orders.new` through to `fills.done`
- Watchdog detects missed heartbeat and attempts restart
- Local buffer drains correctly after Redis reconnect
- Kill switch blocks all orders

---

## Startup Script

`scripts/start_all.sh` should start processes in this order with health checks between each:

1. Redis primary
2. Redis replica
3. Redis Sentinel
4. Archiver (must be running before others publish)
5. Watchdog
6. Risk gate
7. EMSX gateway
8. Strategy / user modules

Each process writes its PID to `pids/{module_name}.pid` for the stop script.

---

## Key Design Constraints — Do Not Violate

- No component imports or calls another component directly
- No component holds a BLPAPI handle except `emsx_gateway`
- No component publishes `orders.approved` except `risk_gate`
- No component writes to the archive except `archiver`
- Blocking I/O always goes through `loop.run_in_executor`
- `message_id` is always a UUID4 generated at publish time, never passed in by the caller
- `owner_id` is always the module's registered name, never overridable
- Every consumed message must be either `XACK`ed or left in PEL — never silently dropped
