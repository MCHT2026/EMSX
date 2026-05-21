"""Summary stats and timeline reports from a BacktestResult."""
from __future__ import annotations

import math
from typing import Any

from .engine import BacktestResult


def _sharpe(returns: list[float], periods_per_year: int = 252 * 390) -> float:
    if not returns:
        return 0.0
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / max(1, len(returns) - 1)
    sd = math.sqrt(var)
    if sd == 0:
        return 0.0
    return (mean / sd) * math.sqrt(periods_per_year)


def _max_drawdown(equity: list[float]) -> float:
    peak = -float("inf")
    max_dd = 0.0
    for v in equity:
        peak = max(peak, v)
        max_dd = min(max_dd, v - peak)
    return max_dd


def generate_report(result: BacktestResult) -> dict[str, Any]:
    equity = [snap.total for _ts, snap in result.timeline]
    if len(equity) >= 2:
        returns = [equity[i] - equity[i - 1] for i in range(1, len(equity))]
    else:
        returns = []
    return {
        "bars": result.bars_processed,
        "targets": result.targets_emitted,
        "intents": result.intents_emitted,
        "fills": result.fills_count,
        "realized_pnl": result.realized_pnl,
        "unrealized_pnl": result.unrealized_pnl,
        "total_pnl": result.total_pnl,
        "total_commission": result.total_commission,
        "net_pnl": result.total_pnl - result.total_commission,
        "sharpe_annualized": _sharpe(returns),
        "max_drawdown": _max_drawdown(equity),
        "end_positions": result.end_positions,
    }
