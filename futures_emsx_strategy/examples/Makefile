.PHONY: list smoke ticks bars history emsx-subs

ROOT := $(shell cd .. && pwd)
PY := PYTHONPATH=$(ROOT) python

list:
	@echo "Examples (run from repo root with: python -m examples.NN_name):"
	@for f in $(sort $(wildcard [0-9]*.py)); do \
		head -1 $$f | sed -e 's|"""||' -e "s|^|  $${f%.py}: |"; \
	done

smoke:
	cd .. && python -m examples.01_blpapi_smoke

ticks:
	cd .. && python -m examples.02_live_ticks --seconds 30

bars:
	cd .. && python -m examples.03_minute_bars --seconds 180

history:
	cd .. && python -m examples.04_historical_backfill --instrument "ESM6 Index"

emsx-subs:
	cd .. && python -m examples.05_emsx_subscriptions --seconds 60
