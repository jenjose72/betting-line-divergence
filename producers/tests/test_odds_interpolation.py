from producers.odds_interpolation import interpolate_odds

OPEN_ODDS = (8.0, 5.5, 1.33)
CLOSE_ODDS = (9.0, 5.25, 1.33)
EVENTS = [
    {"minute": 23, "type": "Goal"},
    {"minute": 41, "type": "Goal"},
    {"minute": 77, "type": "Goal"},
]


def test_returns_one_tick_per_minute():
    ticks = interpolate_odds(OPEN_ODDS, CLOSE_ODDS, EVENTS, duration=90)
    assert len(ticks) == 91
    assert [t["minute"] for t in ticks] == list(range(91))


def test_first_tick_matches_open_odds():
    ticks = interpolate_odds(OPEN_ODDS, CLOSE_ODDS, EVENTS, duration=90)
    first = ticks[0]
    assert abs(first["home_odds"] - OPEN_ODDS[0]) < 0.05
    assert abs(first["draw_odds"] - OPEN_ODDS[1]) < 0.05
    assert abs(first["away_odds"] - OPEN_ODDS[2]) < 0.05


def test_last_tick_matches_close_odds():
    ticks = interpolate_odds(OPEN_ODDS, CLOSE_ODDS, EVENTS, duration=90)
    last = ticks[-1]
    assert abs(last["home_odds"] - CLOSE_ODDS[0]) < 0.01
    assert abs(last["draw_odds"] - CLOSE_ODDS[1]) < 0.01
    assert abs(last["away_odds"] - CLOSE_ODDS[2]) < 0.01


def test_no_events_still_reaches_close_odds():
    ticks = interpolate_odds(OPEN_ODDS, CLOSE_ODDS, [], duration=90)
    assert abs(ticks[-1]["home_odds"] - CLOSE_ODDS[0]) < 0.01


def test_odds_move_sharply_around_an_event_minute():
    """The home odds should move more in the 4 minutes surrounding a goal
    (event-driven jump) than in an equal-width window with no event nearby."""
    ticks = interpolate_odds(OPEN_ODDS, CLOSE_ODDS, EVENTS, duration=90)
    by_minute = {t["minute"]: t["home_odds"] for t in ticks}

    move_around_goal = abs(by_minute[25] - by_minute[21])  # spans the minute-23 goal
    move_quiet_period = abs(by_minute[55] - by_minute[51])  # no event nearby

    assert move_around_goal > move_quiet_period
