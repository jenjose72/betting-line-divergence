import json
from pathlib import Path

from producers.events_producer import normalize_events

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "api_football_raw_events_1035037.json"


def load_raw_events():
    with open(FIXTURE_PATH) as f:
        return json.load(f)


def test_normalizes_real_api_response_shape():
    """Real API-Football response for fixture 1035037 (Burnley 0-3 Man City,
    2023-08-11): 3 goals, 1 red card, 10 subs, 1 Var entry, 15 raw events total."""
    normalized = normalize_events(load_raw_events())
    types = [e["type"] for e in normalized]

    assert types.count("Goal") == 3
    assert types.count("Red Card") == 1
    assert types.count("Substitution") == 10
    assert len(normalized) == 14  # 15 raw events minus the 1 skipped "Var" entry


def test_var_entries_are_dropped():
    normalized = normalize_events(load_raw_events())
    assert all(e["type"] != "Var" for e in normalized)


def test_stoppage_time_events_clip_to_duration():
    normalized = normalize_events(load_raw_events(), duration=90)
    # raw events at elapsed=90, extra=1/3/4/5 should all clip to minute 90
    assert all(e["minute"] <= 90 for e in normalized)
    red_card = next(e for e in normalized if e["type"] == "Red Card")
    assert red_card["minute"] == 90  # was 90+4 in the raw payload


def test_events_sorted_by_minute():
    normalized = normalize_events(load_raw_events())
    minutes = [e["minute"] for e in normalized]
    assert minutes == sorted(minutes)


def test_first_goal_minute_matches_real_match():
    normalized = normalize_events(load_raw_events())
    first_goal = next(e for e in normalized if e["type"] == "Goal")
    assert first_goal["minute"] == 4
    assert first_goal["player"] == "E. Haaland"
