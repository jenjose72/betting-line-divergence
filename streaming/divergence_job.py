"""Spark Structured Streaming job: scores how far a match's actual odds
movement diverges from what its events would explain.

Architecture (see PROJECT_PLAN.md):
    Kafka (odds-ticks, match-events)
        -> Spark Structured Streaming (this file)
        -> HDFS (raw Parquet) + Hive external tables (queryable results)

Code layout
-----------
    JobConfig         -- command-line settings + every HDFS path derived from them
    KafkaTopicParser  -- Kafka JSON messages -> typed, windowed DataFrames
    DivergenceScorer  -- pure batch scoring (validation, de-vigging, the join)
    DivergenceJob     -- the I/O: wires streams, raw sinks and the scoring trigger

Design notes
------------
Raw layer, not a Hive-integrated Spark session: this job writes plain
Parquet to fixed HDFS paths rather than using Spark's Hive catalog
(enableHiveSupport). Hive tables over those same paths are created once,
separately, via hive_tables.sql -- this avoids coupling Spark's bundled
Hive client version to the Hive server's (2.3.2), a common source of
version-mismatch breakage. Hive just reads whatever Parquet Spark has
written; neither side needs to know about the other beyond the shared path.

Two-pass scoring, not a native stream-stream join: Spark 3.1's
stream-stream outer joins are finicky for this shape of problem (both sides
pre-aggregated into windows), and getting the "odds moved with NO matching
event" case right -- the single most important case for this analysis --
needs a left-outer join, the hardest variant to get right. Instead, each
trigger re-reads the full accumulated raw tables as ordinary batch
DataFrames and recomputes the windowed join + score from scratch. At this
project's data scale (a handful of matches, ~100-200 rows each) a full
recompute per trigger is cheap and removes an entire class of
streaming-state bugs, at the cost of not being a "true" incremental
streaming join. Documented tradeoff, not an oversight.

Scoring is driven by a `rate` source heartbeat, not by the odds stream
itself: Structured Streaming's ProcessingTime trigger only fires a new
micro-batch when the *source* has new offsets to read -- it does not run
empty batches on a fixed clock the way the name suggests. Replaying a
match's odds all at once (as happens at --speed 0, or any time a replay
finishes) delivers every row in a single micro-batch; after that, the
odds topic has no new offsets, so nothing would ever trigger process_batch
again, permanently freezing the score table at whatever partial state
existed when raw data first became readable. A built-in `rate` source
(1 row/sec) is used purely as a clock -- its own rows are never touched --
so foreachBatch fires reliably every trigger interval regardless of
whether the real topics have new data, and each firing re-reads the full
accumulated raw tables. Found by testing the actual failure mode (a
same-instant replay), not by inspection.

Independent expected-movement model: EVENT_SHIFTS below is deliberately
NOT the same table as producers/odds_interpolation.EVENT_WEIGHTS. That
table shapes how the simulated "actual" odds move; if this job's notion of
"expected" reused it directly, actual and expected would be mathematically
tied to the same generating process and divergence would trivially
collapse to ~0 everywhere by construction -- an analysis that can't fail
isn't an analysis. EVENT_SHIFTS is this job's own, independently-set prior
for how much each event type "should" move an efficient market, expressed
directly in probability units.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Callable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

WINDOW_MINUTES = 5
BACKGROUND_DRIFT_PER_WINDOW = 0.01  # expected probability drift per window even absent any event
DIVERGENCE_THRESHOLD = 0.05  # flag when actual movement exceeds expected by more than this

# Spark's own prior for expected market impact per event type, in
# probability units. Deliberately independent of producers/odds_interpolation
# -- see module docstring.
EVENT_SHIFTS = {
    "Goal": 0.12,
    "Red Card": 0.06,
    "Penalty Missed": 0.04,
    "Yellow Card": 0.02,
    "Substitution": 0.01,
}
DEFAULT_EVENT_SHIFT = 0.01

# Decimal odds must be strictly above 1.0 to mean anything (1.0 pays back
# only the stake, i.e. a certain outcome); anything at or below it -- or
# null, from a message from_json couldn't parse -- is a corrupt tick.
MIN_VALID_DECIMAL_ODDS = 1.0

# The three outcomes of a 1X2 market. Every per-outcome column is named
# "<prefix>_<outcome>" (home_odds is the one exception, see _odds_col), so
# anything done "once per outcome" loops over this instead of being
# written out three times.
OUTCOMES = ("home", "draw", "away")

# What identifies one scored row: a match and a WINDOW_MINUTES-wide bucket
# of match time. Both tables are grouped and joined on exactly this.
WINDOW_KEY = ["match_id", "window_start"]

LOG_PREFIX = "[divergence-job]"

ODDS_SCHEMA = StructType(
    [
        StructField("match_id", StringType()),
        StructField("minute", IntegerType()),
        StructField("home_odds", DoubleType()),
        StructField("draw_odds", DoubleType()),
        StructField("away_odds", DoubleType()),
        StructField("produced_at", StringType()),
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
    ]
)


def _odds_col(outcome: str) -> Column:
    """The decimal-odds column for one outcome, e.g. "home" -> home_odds."""
    return F.col(f"{outcome}_odds")


def _log(message: str) -> None:
    """Prints one job log line. Flushed immediately so it shows up in the
    Spark driver's output as it happens rather than when a buffer fills."""
    print(f"{LOG_PREFIX} {message}", flush=True)


@dataclass(frozen=True)
class JobConfig:
    """Everything the job needs to know about where it reads and writes.

    Holds the command-line settings and derives every HDFS path from
    hdfs_base, so a path is spelled in exactly one place and the raw sinks,
    the scoring step and their checkpoints can't drift apart.
    """

    bootstrap_servers: str = "kafka:29092"
    odds_topic: str = "odds-ticks"
    events_topic: str = "match-events"
    hdfs_base: str = "hdfs://namenode:8020/data/line_watch"
    trigger_seconds: int = 10

    # Table names under hdfs_base. hive_tables.sql points its external
    # tables at these same directories.
    ODDS_RAW_TABLE = "odds_ticks_raw"
    EVENTS_RAW_TABLE = "match_events_raw"
    SCORES_TABLE = "divergence_scores"

    @classmethod
    def from_args(cls, argv: list[str] | None = None) -> "JobConfig":
        """Builds the config from command-line arguments (sys.argv when
        argv is None). Defaults come from the dataclass fields above."""
        parser = argparse.ArgumentParser(description="Spark Structured Streaming divergence-scoring job.")
        parser.add_argument(
            "--bootstrap-servers",
            default=cls.bootstrap_servers,
            help="Kafka bootstrap servers as seen from inside the Spark containers (use localhost:9092 if running outside Docker).",
        )
        parser.add_argument("--odds-topic", default=cls.odds_topic)
        parser.add_argument("--events-topic", default=cls.events_topic)
        parser.add_argument("--hdfs-base", default=cls.hdfs_base)
        parser.add_argument("--trigger-seconds", type=int, default=cls.trigger_seconds)
        return cls(**vars(parser.parse_args(argv)))

    def table_path(self, table: str) -> str:
        """HDFS directory holding one table's Parquet files."""
        return f"{self.hdfs_base}/{table}"

    def checkpoint_path(self, table: str) -> str:
        """HDFS directory for the checkpoint of the query that writes `table`."""
        return f"{self.hdfs_base}/checkpoints/{table}"

    @property
    def odds_raw_path(self) -> str:
        return self.table_path(self.ODDS_RAW_TABLE)

    @property
    def events_raw_path(self) -> str:
        return self.table_path(self.EVENTS_RAW_TABLE)

    @property
    def scores_path(self) -> str:
        return self.table_path(self.SCORES_TABLE)

    @property
    def trigger_interval(self) -> str:
        """trigger_seconds in the string form Spark's trigger() expects."""
        return f"{self.trigger_seconds} seconds"


class KafkaTopicParser:
    """Turns raw Kafka messages into typed rows tagged with their match window.

    Stateless -- the methods are grouped here because they share one
    parsing step (_parse_json) and only differ in schema and in the extra
    column events get.
    """

    @staticmethod
    def window_start_col() -> Column:
        """Start minute of the WINDOW_MINUTES-wide bucket a row's `minute`
        falls in, e.g. minute 7 -> 5 with 5-minute windows."""
        return (F.floor(F.col("minute") / F.lit(WINDOW_MINUTES)) * F.lit(WINDOW_MINUTES)).cast("int")

    @staticmethod
    def expected_shift_col() -> Column:
        """How much an event of this row's `type` is expected to move the
        market: its EVENT_SHIFTS entry, or DEFAULT_EVENT_SHIFT for a type
        that isn't listed (or is null)."""
        shift_expr = F.lit(DEFAULT_EVENT_SHIFT)
        for event_type, shift in EVENT_SHIFTS.items():
            shift_expr = F.when(F.col("type") == event_type, F.lit(shift)).otherwise(shift_expr)
        return shift_expr

    @classmethod
    def _parse_json(cls, raw_df: DataFrame, schema: StructType) -> DataFrame:
        """Decodes the Kafka `value` bytes as JSON matching `schema`, one
        column per field, and adds window_start. A message that doesn't
        parse comes out as an all-null row rather than raising -- those are
        filtered later by DivergenceScorer, not here, so the raw tables
        stay a faithful record of what arrived."""
        return (
            raw_df.select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
            .select("data.*")
            .withColumn("window_start", cls.window_start_col())
        )

    @classmethod
    def parse_odds(cls, raw_df: DataFrame) -> DataFrame:
        """Parses odds-ticks messages: ODDS_SCHEMA columns + window_start."""
        return cls._parse_json(raw_df, ODDS_SCHEMA)

    @classmethod
    def parse_events(cls, raw_df: DataFrame) -> DataFrame:
        """Parses match-events messages: EVENTS_SCHEMA columns +
        window_start + expected_shift."""
        return cls._parse_json(raw_df, EVENTS_SCHEMA).withColumn("expected_shift", cls.expected_shift_col())


class DivergenceScorer:
    """Scores each match window: how much did the odds actually move,
    versus how much the window's events say they should have?

    Pure batch logic over already-loaded DataFrames -- no Kafka, HDFS or
    streaming -- kept apart from DivergenceJob so it's directly
    unit-testable with a local SparkSession. Stateless; every tunable is a
    module-level constant.
    """

    @staticmethod
    def _has_window_key() -> Column:
        """True for rows that can be placed in a match window at all. The
        check both tables share: an unparseable message has neither."""
        return F.col("match_id").isNotNull() & F.col("window_start").isNotNull()

    @classmethod
    def valid_odds(cls, odds_df: DataFrame) -> DataFrame:
        """Drops ticks that can't be scored. from_json turns an unparseable
        Kafka message into an all-null row rather than failing, so without this
        a malformed message becomes a scored window with a null match_id, and a
        single bad price (e.g. 0.5) yields a bogus implied probability that can
        flag an otherwise flat window. Null comparisons are falsy in filter(),
        so the > check also drops null odds."""
        condition = cls._has_window_key()
        for outcome in OUTCOMES:
            condition = condition & (_odds_col(outcome) > F.lit(MIN_VALID_DECIMAL_ODDS))
        return odds_df.filter(condition)

    @classmethod
    def valid_events(cls, events_df: DataFrame) -> DataFrame:
        """Drops events that can't be placed in a match window. A null `type` is
        kept -- KafkaTopicParser already scores it at DEFAULT_EVENT_SHIFT."""
        return events_df.filter(cls._has_window_key())

    @staticmethod
    def implied_probs(odds_df: DataFrame) -> DataFrame:
        """Adds prob_home/prob_draw/prob_away: each outcome's implied
        probability (1 / decimal odds) divided by the three's sum. That sum
        exceeds 1 by the bookmaker's margin (the overround), so dividing by
        it "de-vigs" the prices into probabilities that add up to 1."""
        overround = sum(1 / _odds_col(outcome) for outcome in OUTCOMES)
        for outcome in OUTCOMES:
            odds_df = odds_df.withColumn(f"prob_{outcome}", (1 / _odds_col(outcome)) / overround)
        return odds_df

    @staticmethod
    def actual_moves(odds_df: DataFrame) -> DataFrame:
        """One row per window with actual_move: the largest swing
        (max - min) in implied probability among the three outcomes across
        the window's ticks. Expects implied_probs() columns."""
        swings = [
            (F.max(f"prob_{outcome}") - F.min(f"prob_{outcome}")).alias(f"move_{outcome}") for outcome in OUTCOMES
        ]
        return (
            odds_df.groupBy(*WINDOW_KEY)
            .agg(*swings, F.count("*").alias("n_ticks"))
            .withColumn("actual_move", F.greatest(*(F.col(f"move_{outcome}") for outcome in OUTCOMES)))
        )

    @staticmethod
    def expected_moves(events_df: DataFrame) -> DataFrame:
        """One row per window that had events: their summed expected
        shift, how many there were, and their types as a comma-separated
        string. Windows with no events simply have no row here."""
        return events_df.groupBy(*WINDOW_KEY).agg(
            F.sum("expected_shift").alias("event_shift_sum"),
            F.count("*").alias("n_events"),
            F.concat_ws(",", F.collect_list("type")).alias("event_types"),
        )

    @classmethod
    def score(cls, odds: DataFrame, events: DataFrame) -> DataFrame:
        """Scores every window that has at least one valid odds tick.

        Validates its inputs itself, since it reads raw tables that may
        already hold corrupt rows. The join is left-outer from the odds
        side so a window whose odds moved with *no* event still gets a row
        -- that is the case this whole analysis exists to catch -- with the
        event columns defaulted to "nothing happened".

        Returns one row per (match_id, window_start), ordered by that key:
        n_ticks, n_events, event_types, actual_move, expected_move,
        divergence_score (actual - expected) and flagged (score above
        DIVERGENCE_THRESHOLD).
        """
        actual = cls.actual_moves(cls.implied_probs(cls.valid_odds(odds)))
        expected = cls.expected_moves(cls.valid_events(events))

        scored = (
            actual.join(expected, on=WINDOW_KEY, how="left")
            .withColumn(
                "expected_move", F.lit(BACKGROUND_DRIFT_PER_WINDOW) + F.coalesce(F.col("event_shift_sum"), F.lit(0.0))
            )
            .withColumn("n_events", F.coalesce(F.col("n_events"), F.lit(0)))
            .withColumn("event_types", F.coalesce(F.col("event_types"), F.lit("")))
            .withColumn("divergence_score", F.col("actual_move") - F.col("expected_move"))
            .withColumn("flagged", F.col("divergence_score") > F.lit(DIVERGENCE_THRESHOLD))
        )

        rounded = [F.round(name, 4).alias(name) for name in ("actual_move", "expected_move", "divergence_score")]
        return scored.select(*WINDOW_KEY, "n_ticks", "n_events", "event_types", *rounded, "flagged").orderBy(
            *WINDOW_KEY
        )


def score_divergence(odds: DataFrame, events: DataFrame) -> DataFrame:
    """Function-style entry point to DivergenceScorer.score(), for callers
    (and tests) that just want the scores for two DataFrames."""
    return DivergenceScorer.score(odds, events)


class DivergenceJob:
    """The streaming job itself: all of the Kafka and HDFS I/O.

    Runs three independent streaming queries -- one landing each Kafka
    topic in HDFS as raw Parquet, and a heartbeat-driven one that rescores
    everything accumulated so far (see module docstring for why scoring
    isn't a stream-stream join and isn't triggered by the odds stream).
    """

    def __init__(self, spark: SparkSession, config: JobConfig):
        self.spark = spark
        self.config = config

    def read_kafka_stream(self, topic: str) -> DataFrame:
        """Streaming DataFrame over one Kafka topic, from its earliest
        offset so a restarted job (or a late-started one) still sees the
        whole match."""
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.config.bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )

    def start_raw_sink(self, stream: DataFrame, table: str) -> StreamingQuery:
        """Starts a query appending a parsed stream to `table`'s Parquet
        directory, checkpointed under the same table name."""
        return (
            stream.writeStream.format("parquet")
            .option("path", self.config.table_path(table))
            .option("checkpointLocation", self.config.checkpoint_path(table))
            .trigger(processingTime=self.config.trigger_interval)
            .outputMode("append")
            .start()
        )

    def start_scoring(self) -> StreamingQuery:
        """Starts the query that calls process_batch once per trigger
        interval. Its source is a `rate` heartbeat, not a real data source
        -- see module docstring for why the scoring trigger can't be the
        odds stream itself."""
        heartbeat = self.spark.readStream.format("rate").option("rowsPerSecond", 1).load()
        return (
            heartbeat.writeStream.foreachBatch(self.process_batch)
            .option("checkpointLocation", self.config.checkpoint_path(self.config.SCORES_TABLE))
            .trigger(processingTime=self.config.trigger_interval)
            .start()
        )

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Rescores all raw data accumulated so far and overwrites the
        scores table with the result.

        batch_df is the heartbeat's own rows -- unused, it exists only to
        get this function called on a fixed clock (see module docstring).
        The three streaming queries in this job start independently and
        race a little, so an early trigger here can hit "raw tables not
        ready yet" -- harmless, since the heartbeat guarantees another
        trigger regardless, unlike gating on the odds stream would.
        """
        try:
            odds = self.spark.read.parquet(self.config.odds_raw_path)
            events = self.spark.read.parquet(self.config.events_raw_path)
        except Exception as exc:  # raw tables may not exist yet on the very first trigger(s)
            _log(f"batch {batch_id}: raw tables not ready yet ({exc})")
            return

        result = DivergenceScorer.score(odds, events)
        result.write.mode("overwrite").parquet(self.config.scores_path)
        flagged_count = result.filter(F.col("flagged")).count()
        _log(f"batch {batch_id}: scored {result.count()} windows, {flagged_count} flagged")

        self._log_skipped_rows(batch_id, odds, events)

    @staticmethod
    def _count_dropped(df: DataFrame, validator: Callable[[DataFrame], DataFrame]) -> int:
        """How many of df's rows `validator` filters out."""
        return df.count() - validator(df).count()

    def _log_skipped_rows(self, batch_id: int, odds: DataFrame, events: DataFrame) -> None:
        """DivergenceScorer drops corrupt rows silently; surface how many so
        a misbehaving producer shows up here instead of as quietly missing
        data. Says nothing when every row was valid."""
        dropped_odds = self._count_dropped(odds, DivergenceScorer.valid_odds)
        dropped_events = self._count_dropped(events, DivergenceScorer.valid_events)
        if dropped_odds or dropped_events:
            _log(f"batch {batch_id}: skipped {dropped_odds} invalid odds ticks, {dropped_events} invalid events")

    def run(self) -> None:
        """Starts all three queries and blocks until any of them stops
        (normally: until the job is killed, or one of them fails)."""
        config = self.config
        odds_stream = KafkaTopicParser.parse_odds(self.read_kafka_stream(config.odds_topic))
        events_stream = KafkaTopicParser.parse_events(self.read_kafka_stream(config.events_topic))

        self.start_raw_sink(odds_stream, config.ODDS_RAW_TABLE)
        self.start_raw_sink(events_stream, config.EVENTS_RAW_TABLE)
        self.start_scoring()

        self.spark.streams.awaitAnyTermination()


def main():
    """Entry point for spark-submit: parse arguments, build the session,
    run the job."""
    config = JobConfig.from_args()

    spark = SparkSession.builder.appName("line-watch-divergence").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    DivergenceJob(spark, config).run()


if __name__ == "__main__":
    main()
