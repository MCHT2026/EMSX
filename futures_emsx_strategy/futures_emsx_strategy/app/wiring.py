"""Centralized wiring that all entry points share."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..config.loader import AppConfig, load_app_config
from ..core.clock import SystemClock
from ..core.logging import configure_logging, get_logger
from ..execution.base import ExecutionAdapter
from ..execution.paper_adapter import PaperExecutionAdapter
from ..market_data.base import MarketDataProvider
from ..market_data.bar_builder import MinuteBarBuilder
from ..market_data.mock_provider import MockMarketDataProvider
from ..market_data.stale_data_monitor import StaleDataMonitor
from ..market_data.tick_store import InMemoryTickStore
from ..messaging import make_bus
from ..messaging.bus import EventBus
from ..monitoring.alerts import ConsoleAlertSink
from ..monitoring.health import HealthChecker
from ..monitoring.metrics import Metrics
from ..orders.idempotency import IdempotencyStore
from ..orders.models import WorkingOrderBook
from ..orders.order_manager import OrderManager
from ..orders.throttles import RateLimiter
from ..portfolio.exposure import ExposureCalculator
from ..portfolio.fills import FillLedger
from ..portfolio.pnl import PnLCalculator
from ..portfolio.positions import PositionBook
from ..risk.kill_switch import KillSwitch
from ..risk.limits import RiskLimits
from ..risk.pre_trade import PreTradeRiskGateway
from ..storage.db import Database
from ..storage.event_log import JsonlEventLog
from ..strategy.minute_strategy import MinuteFuturesStrategy

log = get_logger(__name__)


@dataclass
class Services:
    config: AppConfig
    bus: EventBus
    db: Database
    event_log: JsonlEventLog
    metrics: Metrics
    health: HealthChecker
    alerts: ConsoleAlertSink
    market_data: MarketDataProvider
    bar_builder: MinuteBarBuilder
    tick_store: InMemoryTickStore
    stale_monitor: StaleDataMonitor
    positions: PositionBook
    fills: FillLedger
    pnl: PnLCalculator
    exposure: ExposureCalculator
    working: WorkingOrderBook
    idempotency: IdempotencyStore
    order_manager: OrderManager
    kill_switch: KillSwitch
    risk: PreTradeRiskGateway
    execution: ExecutionAdapter
    strategies: list[MinuteFuturesStrategy]


def build_services(
    config_dir: str | Path,
    *,
    metrics_port: int | None = None,
    market_data: MarketDataProvider | None = None,
    execution: ExecutionAdapter | None = None,
) -> Services:
    config = load_app_config(config_dir)
    configure_logging(level="INFO", json=False)

    bus = make_bus(config.environments.bus, config.environments.bus_url)
    db = Database(config.environments.db_url)
    event_log = JsonlEventLog(config.environments.event_log_path)
    metrics = Metrics(port=metrics_port if metrics_port is not None else None)
    health = HealthChecker()
    alerts = ConsoleAlertSink()

    md = market_data or MockMarketDataProvider()
    bar_builder = MinuteBarBuilder(interval_minutes=config.market_data.bar_interval_minutes)
    tick_store = InMemoryTickStore()
    stale = StaleDataMonitor(max_age_seconds=config.risk_limits.stale_data_seconds)

    positions = PositionBook()
    fills_ledger = FillLedger()
    pnl_calc = PnLCalculator(config.instruments)
    exposure_calc = ExposureCalculator(config.instruments)
    working = WorkingOrderBook()
    idempotency = IdempotencyStore()

    order_manager = OrderManager(positions, working, idempotency)
    kill_switch = KillSwitch(armed=config.risk_limits.kill_switch_armed)
    order_rate = RateLimiter(config.risk_limits.max_orders_per_minute, 60, clock=SystemClock())

    def _get_mark(symbol: str) -> float | None:
        tick = tick_store.last(symbol)
        return tick.last if tick and tick.last is not None else None

    risk = PreTradeRiskGateway(
        limits=RiskLimits(
            max_order_qty=config.risk_limits.max_order_qty,
            max_position=config.risk_limits.max_position,
            max_notional=config.risk_limits.max_notional,
            stale_data_seconds=config.risk_limits.stale_data_seconds,
            require_market_session=config.risk_limits.require_market_session,
            max_orders_per_minute=config.risk_limits.max_orders_per_minute,
            max_cancels_per_minute=config.risk_limits.max_cancels_per_minute,
        ),
        instruments=config.instruments,
        positions=positions,
        exposure=exposure_calc,
        stale_monitor=stale,
        kill_switch=kill_switch,
        order_rate=order_rate,
        get_mark=_get_mark,
    )

    if execution is None:
        execution = PaperExecutionAdapter(
            get_mark=_get_mark,
            tick_size_lookup=lambda s: config.instruments.by_symbol(s).tick_size,
        )

    strategies = [
        MinuteFuturesStrategy(
            strategy_id=s.strategy_id,
            instrument=s.instrument,
            base_qty=s.base_qty,
            params=s.params,
        )
        for s in config.strategies.strategies
    ]

    return Services(
        config=config,
        bus=bus,
        db=db,
        event_log=event_log,
        metrics=metrics,
        health=health,
        alerts=alerts,
        market_data=md,
        bar_builder=bar_builder,
        tick_store=tick_store,
        stale_monitor=stale,
        positions=positions,
        fills=fills_ledger,
        pnl=pnl_calc,
        exposure=exposure_calc,
        working=working,
        idempotency=idempotency,
        order_manager=order_manager,
        kill_switch=kill_switch,
        risk=risk,
        execution=execution,
        strategies=strategies,
    )
