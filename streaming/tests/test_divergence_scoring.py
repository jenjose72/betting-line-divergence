"""Tests score_divergence() as a pure batch function -- no Kafka/HDFS needed,
just a local SparkSession. Constants below are hand-picked so the same
market move (actual_move ~0.10 in both windows) is flagged in one window
(no event explains it) and not flagged in the other (a Goal does) -- that
contrast is the entire point of the analysis, so it's what's tested here.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from streaming.divergence_job import score_divergence

ODDS_COLUMNS = ["match_id", "minute", "home_odds", "draw_odds", "away_odds", "produced_at", "window_start"]
EVENTS_COLUMNS = ["match_id", "minute", "type", "team", "player", "produced_at", "window_start", "expected_shift"]

ODDS_SCHEMA = StructType(
    [
        StructField("match_id", StringType()),
        StructField("minute", IntegerType()),
        StructField("home_odds", DoubleType()),
        StructField("draw_odds", DoubleType()),
        StructField("away_odds", DoubleType()),
        StructField("produced_at", StringType()),
        StructField("window_start", IntegerType()),
    ]
)

EVENTS_SCHEMA = StructType(
    [
        StructField("match_id", StringType()),
        StructField("minute", IntegerType()),
        StructField("type", StringType()),
        StructField("team", StringType()),
        StructField("player", StringType()),
        StructField("produced_at", StringType()),
        StructField("window_start", IntegerType()),
        StructField("expected_shift", DoubleType()),
    ]
)

# home_odds drifting 2.0 -> 3.0 while draw/away stay at 4.0 produces
# prob_home moving ~0.10 (see module docstring in divergence_job.py's tests
# derivation) -- used identically in both windows below.
HOME_ODDS_SEQUENCE = [2.0, 2.1, 2.3, 2.6, 3.0]


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-divergence-scoring").master("local[1]").getOrCreate()
    yield session
    session.stop()


def _odds_rows(match_id: str, window_start: int):
    return [
        (match_id, window_start + i, home_odds, 4.0, 4.0, "2026-01-01T00:00:00Z", window_start)
        for i, home_odds in enumerate(HOME_ODDS_SEQUENCE)
    ]


def test_unexplained_move_is_flagged_but_goal_explained_move_is_not(spark):
    odds_rows = _odds_rows("match-1", window_start=0) + _odds_rows("match-1", window_start=5)
    odds = spark.createDataFrame(odds_rows, ODDS_COLUMNS)

    # Only window 5 gets an event -- window 0 has the same-sized odds move but nothing explaining it.
    events = spark.createDataFrame(
        [("match-1", 6, "Goal", "Home Team", "Player A", "2026-01-01T00:00:00Z", 5, 0.12)],
        EVENTS_COLUMNS,
    )

    result = {row["window_start"]: row for row in score_divergence(odds, events).collect()}

    window0, window5 = result[0], result[5]

    assert window0["actual_move"] == pytest.approx(window5["actual_move"], abs=0.001)
    assert window0["n_events"] == 0
    assert window5["n_events"] == 1
    assert window5["event_types"] == "Goal"

    assert window0["flagged"] is True
    assert window5["flagged"] is False


def test_window_with_no_odds_ticks_is_absent_from_results(spark):
    odds = spark.createDataFrame(_odds_rows("match-1", window_start=0), ODDS_COLUMNS)
    events = spark.createDataFrame([], EVENTS_SCHEMA)

    result = score_divergence(odds, events).collect()

    assert len(result) == 1
    assert result[0]["window_start"] == 0
    assert result[0]["n_events"] == 0
    assert result[0]["event_types"] == ""


# What from_json yields for an unparseable Kafka message: every field null.
MALFORMED_ODDS_ROW = (None, None, None, None, None, None, None)
MALFORMED_EVENT_ROW = (None, None, None, None, None, None, None, None)


def _flat_odds_rows(match_id: str, window_start: int):
    return [
        (match_id, window_start + i, 2.5, 3.2, 3.0, "2026-01-01T00:00:00Z", window_start)
        for i in range(5)
    ]


def test_malformed_rows_are_not_scored_as_a_null_match(spark):
    odds = spark.createDataFrame(_odds_rows("match-1", window_start=0) + [MALFORMED_ODDS_ROW], ODDS_SCHEMA)
    events = spark.createDataFrame([MALFORMED_EVENT_ROW], EVENTS_SCHEMA)

    result = score_divergence(odds, events).collect()

    assert [(row["match_id"], row["window_start"]) for row in result] == [("match-1", 0)]
    assert result[0]["n_ticks"] == len(HOME_ODDS_SEQUENCE)
    assert result[0]["n_events"] == 0


@pytest.mark.parametrize(
    "bad_prices",
    [
        (0.5, 3.2, 3.0),  # below 1.0: implied probability > 100%
        (1.0, 3.2, 3.0),  # exactly 1.0: a "certain" outcome, not a real price
        (2.5, 0.0, 3.0),  # zero: division by zero
        (2.5, None, 3.0),  # missing field
    ],
)
def test_invalid_odds_tick_is_ignored_not_scored(spark, bad_prices):
    # A market that doesn't move at all, plus one corrupt tick. Scoring the
    # 0.5 tick would register a ~0.37 swing in prob_home and flag the window.
    bad_row = ("match-1", 2, *bad_prices, "2026-01-01T00:00:00Z", 0)
    odds = spark.createDataFrame(_flat_odds_rows("match-1", window_start=0) + [bad_row], ODDS_SCHEMA)
    events = spark.createDataFrame([], EVENTS_SCHEMA)

    result = score_divergence(odds, events).collect()

    assert len(result) == 1
    assert result[0]["n_ticks"] == 5
    assert result[0]["actual_move"] == pytest.approx(0.0, abs=1e-9)
    assert result[0]["flagged"] is False


def test_event_with_unknown_type_is_still_counted(spark):
    odds = spark.createDataFrame(_flat_odds_rows("match-1", window_start=0), ODDS_SCHEMA)
    events = spark.createDataFrame(
        [("match-1", 3, None, None, None, "2026-01-01T00:00:00Z", 0, 0.01)],
        EVENTS_SCHEMA,
    )

    result = score_divergence(odds, events).collect()

    assert result[0]["n_events"] == 1
    assert result[0]["expected_move"] == pytest.approx(0.02)
