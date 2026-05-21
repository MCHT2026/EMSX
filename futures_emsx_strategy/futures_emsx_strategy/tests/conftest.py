"""Shared pytest fixtures."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from futures_emsx_strategy.config.loader import (
    EMSXConfig,
    EnvironmentsConfig,
    InstrumentConfig,
    InstrumentsConfig,
    MarketDataConfig,
    RiskLimitsConfig,
    StrategiesConfig,
    StrategyConfig,
)


@pytest.fixture
def instruments_cfg() -> InstrumentsConfig:
    return InstrumentsConfig(
        instruments=[
            InstrumentConfig(
                symbol="ESM6 Index",
                bloomberg_topic="ESM6 Index",
                exchange="CME",
                currency="USD",
                tick_size=0.25,
                point_value=50.0,
                min_qty=1,
                max_qty=50,
                roll_days_before_expiry=8,
                session_tz="America/Chicago",
                session_open="00:00",
                session_close="23:59",
            )
        ]
    )


@pytest.fixture
def strategy_cfg() -> StrategiesConfig:
    return StrategiesConfig(
        strategies=[
            StrategyConfig(
                strategy_id="minute_es_v1",
                type="minute_momentum",
                instrument="ESM6 Index",
                interval_minutes=1,
                base_qty=5,
                params={"fast_lookback": 2, "slow_lookback": 4, "max_position_contracts": 8},
            )
        ]
    )


@pytest.fixture
def risk_cfg() -> RiskLimitsConfig:
    return RiskLimitsConfig(
        max_order_qty=20,
        max_position=50,
        max_notional=10_000_000.0,
        max_orders_per_minute=1000,
        max_cancels_per_minute=1000,
        stale_data_seconds=3600,
        require_market_session=False,
        kill_switch_armed=True,
    )


@pytest.fixture
def emsx_cfg() -> EMSXConfig:
    return EMSXConfig(
        host="localhost",
        port=8194,
        service="//blp/emapisvc_beta",
        broker="BMTB",
        account="TEST",
    )


@pytest.fixture
def market_data_cfg() -> MarketDataConfig:
    return MarketDataConfig(
        provider="mock",
        fields=["BID", "ASK", "LAST_PRICE"],
        bar_interval_minutes=1,
    )


@pytest.fixture
def env_cfg(tmp_path) -> EnvironmentsConfig:
    return EnvironmentsConfig(
        name="test",
        paper_trading=True,
        db_url=f"sqlite:///{tmp_path}/test.db",
        event_log_path=str(tmp_path / "events.jsonl"),
        bus="memory",
        bus_url=None,
        metrics_port=0,
    )


@pytest.fixture
def utc_now() -> datetime:
    return datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc)
