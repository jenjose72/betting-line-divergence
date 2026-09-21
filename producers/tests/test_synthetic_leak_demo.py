from producers.synthetic_leak_demo import FINAL_CLOSE, POST_LEAK, QUIET_MARKET, build_ticks


def test_returns_one_tick_per_minute():
    ticks = build_ticks()
    assert len(ticks) == 91
    assert [t["minute"] for t in ticks] == list(range(91))


def test_market_is_flat_before_the_leak():
    ticks = build_ticks()
    by_minute = {t["minute"]: t for t in ticks}
    for m in (0, 10, 20, 34):
        assert by_minute[m]["home_odds"] == QUIET_MARKET[0]


def test_leak_window_ends_at_the_new_level():
    ticks = build_ticks()
    by_minute = {t["minute"]: t for t in ticks}
    assert by_minute[39]["home_odds"] == POST_LEAK[0]
    assert by_minute[45]["home_odds"] == POST_LEAK[0]  # holds after the leak, before the goal


def test_leak_is_a_large_move_with_no_event_nearby():
    """The whole point of this fixture: minutes 35-39 have no event, but a
    huge odds move -- exactly the pattern the detector exists to catch."""
    ticks = build_ticks()
    by_minute = {t["minute"]: t for t in ticks}
    drop = by_minute[34]["home_odds"] - by_minute[39]["home_odds"]
    assert drop > 1.0  # 3.0 -> 1.8 is a 1.2 decimal-odds crash in 5 minutes


def test_ends_at_final_close():
    ticks = build_ticks()
    assert ticks[-1]["home_odds"] == FINAL_CLOSE[0]
    assert ticks[-1]["draw_odds"] == FINAL_CLOSE[1]
    assert ticks[-1]["away_odds"] == FINAL_CLOSE[2]
