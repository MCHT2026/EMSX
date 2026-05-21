"""Pre-trade risk gateway.

Runs every check on every OrderIntent. ANY failure rejects the order.
Checks include: kill switch, max order qty, max position, max notional,
market session, market data staleness, working-orders rate limit.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ..config.loader import InstrumentsConfig
from ..core.clock import Clock, SystemClock
from ..core.enums import Side
from ..core.events import OrderIntent, RiskDecision
from ..core.logging import get_logger
from ..market_data.stale_data_monitor import StaleDataMonitor
from ..orders.throttles import RateLimiter
from ..portfolio.exposure import ExposureCalculator
from ..portfolio.positions import PositionBook
from .kill_switch import KillSwitch
from .limits import RiskLimits
from .session_checks import SessionChecker
from .validations import ValidationCheck

log = get_logger(__name__)


@dataclass(frozen=True)
class CheckResult:
    decision: RiskDecision
    checks: tuple[ValidationCheck, ...]


class PreTradeRiskGateway:
    def __init__(
        self,
        limits: RiskLimits,
        instruments: InstrumentsConfig,
        positions: PositionBook,
        exposure: ExposureCalculator,
        stale_monitor: StaleDataMonitor,
        kill_switch: KillSwitch,
        order_rate: RateLimiter,
        get_mark: callable | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.limits = limits
        self.instruments = instruments
        self.positions = positions
        self.exposure = exposure
        self.stale_monitor = stale_monitor
        self.kill_switch = kill_switch
        self.order_rate = order_rate
        self.get_mark = get_mark or (lambda _sym: None)
        self.clock = clock or SystemClock()
        self._sessions: dict[str, SessionChecker] = {}

    def _session_checker(self, symbol: str) -> SessionChecker:
        if symbol not in self._sessions:
            self._sessions[symbol] = SessionChecker(self.instruments.by_symbol(symbol))
        return self._sessions[symbol]

    def validate(self, intent: OrderIntent) -> CheckResult:
        now = self.clock.now()
        checks: list[ValidationCheck] = [
            self._check_kill_switch(),
            self._check_max_order_qty(intent),
            self._check_max_position(intent),
            self._check_max_notional(intent),
            self._check_session(intent, now),
            self._check_stale_data(intent),
            self._check_rate_limit(),
        ]
        failures = [c for c in checks if not c.passed]
        if failures:
            decision = RiskDecision.reject(
                intent.idempotency_key,
                [f"{c.name}:{c.reason}" for c in failures],
                decided_at=now,
            )
            log.warning(
                "risk_rejected",
                key=intent.idempotency_key,
                instrument=intent.instrument,
                reasons=decision.reasons,
            )
            return CheckResult(decision=decision, checks=tuple(checks))
        decision = RiskDecision.approve(intent.idempotency_key, decided_at=now)
        log.info("risk_approved", key=intent.idempotency_key, instrument=intent.instrument)
        return CheckResult(decision=decision, checks=tuple(checks))

    def _check_kill_switch(self) -> ValidationCheck:
        if self.kill_switch.is_tripped:
            return ValidationCheck.fail("kill_switch", self.kill_switch.reason or "tripped")
        return ValidationCheck.ok("kill_switch")

    def _check_max_order_qty(self, intent: OrderIntent) -> ValidationCheck:
        if intent.qty <= 0:
            return ValidationCheck.fail("max_order_qty", "qty<=0")
        if intent.qty > self.limits.max_order_qty:
            return ValidationCheck.fail(
                "max_order_qty",
                f"qty={intent.qty} > limit={self.limits.max_order_qty}",
            )
        return ValidationCheck.ok("max_order_qty")

    def _check_max_position(self, intent: OrderIntent) -> ValidationCheck:
        current = self.positions.position(intent.instrument)
        signed = intent.qty * intent.side.sign
        projected = abs(current + signed)
        if projected > self.limits.max_position:
            return ValidationCheck.fail(
                "max_position",
                f"projected={projected} > limit={self.limits.max_position}",
            )
        return ValidationCheck.ok("max_position")

    def _check_max_notional(self, intent: OrderIntent) -> ValidationCheck:
        mark = self.get_mark(intent.instrument) or intent.limit_price
        if mark is None:
            return ValidationCheck.ok("max_notional")
        notional = self.exposure.notional(intent.instrument, intent.qty, mark)
        if notional > self.limits.max_notional:
            return ValidationCheck.fail(
                "max_notional",
                f"notional={notional:.2f} > limit={self.limits.max_notional:.2f}",
            )
        return ValidationCheck.ok("max_notional")

    def _check_session(self, intent: OrderIntent, now: datetime) -> ValidationCheck:
        if not self.limits.require_market_session:
            return ValidationCheck.ok("market_session")
        if self._session_checker(intent.instrument).is_open(now):
            return ValidationCheck.ok("market_session")
        return ValidationCheck.fail("market_session", "outside_session")

    def _check_stale_data(self, intent: OrderIntent) -> ValidationCheck:
        if self.stale_monitor.is_stale(intent.instrument):
            age = self.stale_monitor.age_seconds(intent.instrument)
            return ValidationCheck.fail("stale_market_data", f"age={age}")
        return ValidationCheck.ok("stale_market_data")

    def _check_rate_limit(self) -> ValidationCheck:
        if not self.order_rate.try_acquire():
            return ValidationCheck.fail(
                "order_rate_limit",
                f"max={self.order_rate.max_events}/{self.order_rate.window.total_seconds()}s",
            )
        return ValidationCheck.ok("order_rate_limit")

    @staticmethod
    def projected_side(intent: OrderIntent) -> Side:
        return intent.side
