# Operator dashboards

Prometheus metrics are exposed on `:9100/metrics` (port from `environments.yaml`). Suggested Grafana panels:

## Trading
- `rate(fes_orders_submitted[1m])` by instrument/side
- `rate(fes_orders_rejected[1m])` by reason
- `rate(fes_fills_total[1m])` by instrument
- `fes_open_position` by instrument
- `fes_realized_pnl`, `fes_unrealized_pnl`

## Market data
- `rate(fes_ticks_total[1m])` by instrument
- `fes_market_data_age_seconds` by instrument (alert > 30s)
- `rate(fes_bars_total[1m])` by instrument

## Latency
- `histogram_quantile(0.95, rate(fes_bar_to_order_seconds_bucket[5m]))`
- `histogram_quantile(0.99, rate(fes_bar_to_order_seconds_bucket[5m]))`

## Alerts
- `fes_kill_switch_tripped == 1` (page on transition 0→1)
- `fes_market_data_age_seconds > 30` for > 1m
- `rate(fes_orders_rejected[5m]) > 1` (sustained rejections)
