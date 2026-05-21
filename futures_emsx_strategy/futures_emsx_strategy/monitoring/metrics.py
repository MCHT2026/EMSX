"""Prometheus metrics. Counters, gauges, histograms exposed on /metrics.

Each ``Metrics`` instance owns a dedicated ``CollectorRegistry`` so the same
process can construct multiple service graphs (tests, dev REPLs, warm-restart
flows) without colliding on the prometheus global registry. The HTTP endpoint,
when enabled, serves this registry.
"""
from __future__ import annotations

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    make_wsgi_app,
)
from wsgiref.simple_server import make_server
import threading


class Metrics:
    def __init__(self, port: int | None = 9100) -> None:
        self.registry = CollectorRegistry()
        labels = {"registry": self.registry}
        self.ticks_total = Counter("fes_ticks_total", "Ticks received", ["instrument"], **labels)
        self.bars_total = Counter("fes_bars_total", "Bars emitted", ["instrument"], **labels)
        self.signals_total = Counter("fes_signals_total", "Signals emitted", ["strategy"], **labels)
        self.orders_submitted = Counter("fes_orders_submitted", "Orders submitted", ["instrument", "side"], **labels)
        self.orders_rejected = Counter("fes_orders_rejected", "Orders rejected by risk", ["reason"], **labels)
        self.fills_total = Counter("fes_fills_total", "Fills received", ["instrument"], **labels)

        self.open_position = Gauge("fes_open_position", "Open position by instrument", ["instrument"], **labels)
        self.working_qty = Gauge("fes_working_qty", "Working order qty by instrument", ["instrument"], **labels)
        self.realized_pnl = Gauge("fes_realized_pnl", "Realized PnL", **labels)
        self.unrealized_pnl = Gauge("fes_unrealized_pnl", "Unrealized PnL", **labels)
        self.kill_switch = Gauge("fes_kill_switch_tripped", "1 if tripped, else 0", **labels)

        self.bar_to_order_latency = Histogram(
            "fes_bar_to_order_seconds",
            "Latency from bar close to order send",
            buckets=(0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
            **labels,
        )
        self.market_data_age = Gauge("fes_market_data_age_seconds", "Age of last tick", ["instrument"], **labels)

        self._server: "object | None" = None
        if port is not None and port > 0:
            app = make_wsgi_app(registry=self.registry)
            server = make_server("0.0.0.0", port, app)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self._server = server
