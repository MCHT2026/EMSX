from futures_emsx_strategy.strategy.indicators import EMA, SMA, RollingZ


def test_sma_ready_after_period():
    s = SMA(3)
    assert s.update(1.0) is None
    assert s.update(2.0) is None
    assert s.update(3.0) == 2.0
    assert s.update(4.0) == 3.0


def test_ema_smooths():
    e = EMA(3)
    v = None
    for x in [1.0, 2.0, 3.0, 4.0, 5.0]:
        v = e.update(x)
    assert v is not None
    assert 3.0 < v < 5.0


def test_rolling_z_zero_when_constant():
    z = RollingZ(3)
    z.update(1.0)
    z.update(1.0)
    out = z.update(1.0)
    assert out == 0.0
