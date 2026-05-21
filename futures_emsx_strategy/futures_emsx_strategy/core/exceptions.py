class FESError(Exception):
    """Base exception for futures_emsx_strategy."""


class ConfigError(FESError):
    pass


class MarketDataError(FESError):
    pass


class StaleMarketDataError(MarketDataError):
    pass


class StrategyError(FESError):
    pass


class OrderError(FESError):
    pass


class DuplicateOrderError(OrderError):
    pass


class RiskRejection(FESError):
    def __init__(self, reasons: list[str]) -> None:
        super().__init__("; ".join(reasons))
        self.reasons = reasons


class ExecutionError(FESError):
    pass


class EMSXError(ExecutionError):
    pass


class ReconciliationBreak(FESError):
    pass
