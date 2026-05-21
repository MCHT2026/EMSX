"""Prometheus metrics. Counters, gauges, histograms exposed on /metrics."""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server


class Metrics:
    def __init__(self, port: int | None = 9100) -> None:
        self.ticks_total = Counter("fes_ticks_total", "Ticks received", ["instrument"])
        self.bars_total = Counter("fes_bars_total", "Bars emitted", ["instrument"])
        self.signals_total = Counter("fes_signals_total", "Signals emitted", ["strategy"])
        self.orders_submitted = Counter("fes_orders_submitted", "Orders submitted", ["instrument", "side"])
        self.orders_rejected = Counter("fes_orders_rejected", "Orders rejected by risk", ["reason"])
        self.fills_total = Counter("fes_fills_total", "Fills received", ["instrument"])

        self.open_position = Gauge("fes_open_position", "Open position by instrument", ["instrument"])
        self.working_qty = Gauge("fes_working_qty", "Working order qty by instrument", ["instrument"])
        self.realized_pnl = Gauge("fes_realized_pnl", "Realized PnL")
        self.unrealized_pnl = Gauge("fes_unrealized_pnl", "Unrealized PnL")
        self.kill_switch = Gauge("fes_kill_switch_tripped", "1 if tripped, else 0")

        self.bar_to_order_latency = Histogram(
            "fes_bar_to_order_seconds",
            "Latency from bar close to order send",
            buckets=(0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
        )
        self.market_data_age = Gauge("fes_market_data_age_seconds", "Age of last tick", ["instrument"])

        if port is not None:
            start_http_server(port)
