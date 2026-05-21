"""BloombergMarketDataProvider must honor market_data.yaml and translate
bloomberg_topic -> internal symbol when reporting ticks."""
from __future__ import annotations

import futures_emsx_strategy.market_data.bloomberg_provider as bp


def test_topic_to_symbol_translation_on_dispatch():
    # Stub out the blpapi requirement so the constructor doesn't raise.
    bp._HAVE_BLPAPI = True
    try:
        provider = bp.BloombergMarketDataProvider(
            host="bpipe.example.com",
            port=8194,
            service="//blp/mktdata-pipe",
            historical_service="//blp/refdata-pipe",
            topic_to_symbol={"ESM6 Index": "ES1"},
        )
        assert provider.host == "bpipe.example.com"
        assert provider.service == "//blp/mktdata-pipe"
        assert provider.historical_service == "//blp/refdata-pipe"

        # Use a stand-in for the BLPAPI message so we don't need a real session.
        class _StubElem:
            def __init__(self, val): self._v = val
            def getValue(self): return self._v
            def getValueAsDatetime(self): return None

        class _StubMsg:
            def __init__(self, topic, fields):
                self._topic = topic
                self._fields = fields
            def correlationIds(self):
                return [type("CID", (), {"value": lambda self_: self._topic})()
                        for _ in (None,)]
            def hasElement(self, name): return name in self._fields
            def getElement(self, name): return _StubElem(self._fields[name])

        provider._dispatch_tick.__wrapped__ if hasattr(provider._dispatch_tick, "__wrapped__") else None

        captured = []
        provider.on_tick(captured.append)

        msg = _StubMsg("ESM6 Index", {"BID": 100.0, "ASK": 100.5, "LAST_PRICE": 100.25, "VOLUME": 1})
        provider._dispatch_tick(msg)
        assert captured, "callback should have fired"
        assert captured[0].instrument == "ES1", "topic must be translated to symbol"
    finally:
        bp._HAVE_BLPAPI = bp._HAVE_BLPAPI  # leave flag alone


def test_topic_translation_defaults_to_topic_when_unmapped():
    bp._HAVE_BLPAPI = True
    provider = bp.BloombergMarketDataProvider(topic_to_symbol={})

    class _StubElem:
        def __init__(self, val): self._v = val
        def getValue(self): return self._v
        def getValueAsDatetime(self): return None

    class _StubMsg:
        def correlationIds(self_):
            return [type("CID", (), {"value": lambda self: "FOO Index"})()]
        def hasElement(self_, name): return name == "LAST_PRICE"
        def getElement(self_, name): return _StubElem(42.0)

    captured = []
    provider.on_tick(captured.append)
    provider._dispatch_tick(_StubMsg())
    assert captured[0].instrument == "FOO Index"
