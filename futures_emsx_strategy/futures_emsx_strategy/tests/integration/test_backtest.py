from datetime import datetime, timedelta, timezone

from futures_emsx_strategy.backtest.engine import BacktestEngine
from futures_emsx_strategy.backtest.reports import generate_report
from futures_emsx_strategy.core.events import BarClosed
from futures_emsx_strategy.strategy.minute_strategy import MinuteFuturesStrategy


def _bar(t: datetime, close: float) -> BarClosed:
    return BarClosed(
        instrument="ESM6 Index",
        start_time=t,
        end_time=t + timedelta(minutes=1),
        open=close - 0.5,
        high=close + 0.5,
        low=close - 1.0,
        close=close,
        volume=10,
        interval_minutes=1,
    )


def test_backtest_runs_and_reports(instruments_cfg):
    strat = MinuteFuturesStrategy(
        strategy_id="bt",
        instrument="ESM6 Index",
        base_qty=2,
        params={"fast_lookback": 2, "slow_lookback": 4, "max_position_contracts": 5},
    )
    engine = BacktestEngine(strat, instruments_cfg)
    t0 = datetime(2026, 5, 20, 14, 0, tzinfo=timezone.utc)
    bars = [_bar(t0 + timedelta(minutes=i), 100.0 + i * 0.5) for i in range(50)]
    result = engine.run(bars)
    report = generate_report(result)
    assert result.bars_processed == 50
    assert report["bars"] == 50
    assert "sharpe_annualized" in report
