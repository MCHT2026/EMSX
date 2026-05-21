"""Loads and validates YAML configuration files."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, model_validator

from ..core.exceptions import ConfigError


def load_yaml(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"Config file not found: {p}")
    with p.open("r") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"Top-level YAML in {p} must be a mapping")
    return data


class InstrumentConfig(BaseModel):
    symbol: str
    bloomberg_topic: str
    exchange: str
    currency: str
    tick_size: float
    point_value: float
    min_qty: int = 1
    max_qty: int = 100
    roll_days_before_expiry: int = 5
    session_tz: str = "America/Chicago"
    session_open: str = "08:30"
    session_close: str = "15:15"


class InstrumentsConfig(BaseModel):
    instruments: list[InstrumentConfig]

    def by_symbol(self, symbol: str) -> InstrumentConfig:
        for ins in self.instruments:
            if ins.symbol == symbol:
                return ins
        raise ConfigError(f"Instrument not configured: {symbol}")


class StrategyConfig(BaseModel):
    strategy_id: str
    type: str
    instrument: str
    interval_minutes: int = 1
    base_qty: int = 1
    params: dict[str, Any] = Field(default_factory=dict)


class StrategiesConfig(BaseModel):
    strategies: list[StrategyConfig]


class RiskLimitsConfig(BaseModel):
    max_order_qty: int
    max_position: int
    max_notional: float
    max_orders_per_minute: int = 10
    max_cancels_per_minute: int = 20
    stale_data_seconds: int = 30
    require_market_session: bool = True
    kill_switch_armed: bool = True


class EMSXConfig(BaseModel):
    host: str = "localhost"
    port: int = 8194
    service: str = "//blp/emapisvc_beta"
    use_server_api: bool = True
    trader_uuid: int | None = None
    account: str | None = None
    broker: str = "BMTB"
    handling_instruction: str = "ANY"
    strategy: str = "VWAP"
    default_order_type: str = "MKT"
    default_tif: str = "DAY"

    @model_validator(mode="after")
    def _validate_service(self) -> "EMSXConfig":
        if not self.service.startswith("//blp/"):
            raise ConfigError(f"EMSX service must start with //blp/: {self.service}")
        return self


class MarketDataConfig(BaseModel):
    provider: str = "bloomberg"
    host: str = "localhost"
    port: int = 8194
    service: str = "//blp/mktdata"
    fields: list[str] = Field(default_factory=lambda: ["BID", "ASK", "LAST_PRICE", "VOLUME"])
    historical_service: str = "//blp/refdata"
    bar_interval_minutes: int = 1


class EnvironmentsConfig(BaseModel):
    name: str = "dev"
    paper_trading: bool = True
    db_url: str = "sqlite:///fes.db"
    event_log_path: str = "./.logs/events.jsonl"
    bus: str = "memory"
    bus_url: str | None = None
    metrics_port: int = 9100


class AppConfig(BaseModel):
    environments: EnvironmentsConfig
    instruments: InstrumentsConfig
    strategies: StrategiesConfig
    risk_limits: RiskLimitsConfig
    emsx: EMSXConfig
    market_data: MarketDataConfig


def load_app_config(config_dir: str | Path) -> AppConfig:
    d = Path(config_dir)
    if not d.is_dir():
        raise ConfigError(f"Config directory not found: {d}")
    return AppConfig(
        environments=EnvironmentsConfig(**load_yaml(d / "environments.yaml")),
        instruments=InstrumentsConfig(**load_yaml(d / "instruments.yaml")),
        strategies=StrategiesConfig(**load_yaml(d / "strategies.yaml")),
        risk_limits=RiskLimitsConfig(**load_yaml(d / "risk_limits.yaml")),
        emsx=EMSXConfig(**load_yaml(d / "emsx.yaml")),
        market_data=MarketDataConfig(**load_yaml(d / "market_data.yaml")),
    )
