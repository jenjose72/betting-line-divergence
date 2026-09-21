"""Spark Structured Streaming job: scores how far a match's actual odds
movement diverges from what its events would explain.

Architecture (see PROJECT_PLAN.md):
    Kafka (odds-ticks, match-events)
        -> Spark Structured Streaming (this file)
        -> HDFS (raw Parquet) + Hive external tables (queryable results)

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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
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


def read_kafka_stream(spark: SparkSession, bootstrap_servers: str, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )


def _window_start_col():
    return (F.floor(F.col("minute") / F.lit(WINDOW_MINUTES)) * F.lit(WINDOW_MINUTES)).cast("int")


def parse_odds(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df.select(F.from_json(F.col("value").cast("string"), ODDS_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("window_start", _window_start_col())
    )


def parse_events(raw_df: DataFrame) -> DataFrame:
    shift_expr = F.lit(DEFAULT_EVENT_SHIFT)
    for event_type, shift in EVENT_SHIFTS.items():
        shift_expr = F.when(F.col("type") == event_type, F.lit(shift)).otherwise(shift_expr)

    return (
        raw_df.select(F.from_json(F.col("value").cast("string"), EVENTS_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("window_start", _window_start_col())
        .withColumn("expected_shift", shift_expr)
    )


def implied_probs(odds_df: DataFrame) -> DataFrame:
    raw_h = 1 / F.col("home_odds")
    raw_d = 1 / F.col("draw_odds")
    raw_a = 1 / F.col("away_odds")
    overround = raw_h + raw_d + raw_a
    return (
        odds_df.withColumn("prob_home", raw_h / overround)
        .withColumn("prob_draw", raw_d / overround)
        .withColumn("prob_away", raw_a / overround)
    )


def score_divergence(odds: DataFrame, events: DataFrame) -> DataFrame:
    """Pure function over two already-loaded batch DataFrames -- kept
    separate from I/O so it's directly unit-testable."""
    odds = implied_probs(odds)

    actual_moves = (
        odds.groupBy("match_id", "window_start")
        .agg(
            (F.max("prob_home") - F.min("prob_home")).alias("move_home"),
            (F.max("prob_draw") - F.min("prob_draw")).alias("move_draw"),
            (F.max("prob_away") - F.min("prob_away")).alias("move_away"),
            F.count("*").alias("n_ticks"),
        )
        .withColumn("actual_move", F.greatest(F.col("move_home"), F.col("move_draw"), F.col("move_away")))
    )

    expected_moves = events.groupBy("match_id", "window_start").agg(
        F.sum("expected_shift").alias("event_shift_sum"),
        F.count("*").alias("n_events"),
        F.concat_ws(",", F.collect_list("type")).alias("event_types"),
    )

    joined = actual_moves.join(expected_moves, on=["match_id", "window_start"], how="left")
    joined = (
        joined.withColumn(
            "expected_move", F.lit(BACKGROUND_DRIFT_PER_WINDOW) + F.coalesce(F.col("event_shift_sum"), F.lit(0.0))
        )
        .withColumn("n_events", F.coalesce(F.col("n_events"), F.lit(0)))
        .withColumn("event_types", F.coalesce(F.col("event_types"), F.lit("")))
    )
    joined = joined.withColumn("divergence_score", F.col("actual_move") - F.col("expected_move"))
    joined = joined.withColumn("flagged", F.col("divergence_score") > F.lit(DIVERGENCE_THRESHOLD))

    return joined.select(
        "match_id",
        "window_start",
        "n_ticks",
        "n_events",
        "event_types",
        F.round("actual_move", 4).alias("actual_move"),
        F.round("expected_move", 4).alias("expected_move"),
        F.round("divergence_score", 4).alias("divergence_score"),
        "flagged",
    ).orderBy("match_id", "window_start")


def main():
    parser = argparse.ArgumentParser(description="Spark Structured Streaming divergence-scoring job.")
    parser.add_argument(
        "--bootstrap-servers",
        default="kafka:29092",
        help="Kafka bootstrap servers as seen from inside the Spark containers (use localhost:9092 if running outside Docker).",
    )
    parser.add_argument("--odds-topic", default="odds-ticks")
    parser.add_argument("--events-topic", default="match-events")
    parser.add_argument("--hdfs-base", default="hdfs://namenode:8020/data/line_watch")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("line-watch-divergence").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    odds_raw_path = f"{args.hdfs_base}/odds_ticks_raw"
    events_raw_path = f"{args.hdfs_base}/match_events_raw"
    scores_path = f"{args.hdfs_base}/divergence_scores"

    odds_stream = parse_odds(read_kafka_stream(spark, args.bootstrap_servers, args.odds_topic))
    events_stream = parse_events(read_kafka_stream(spark, args.bootstrap_servers, args.events_topic))

    odds_writer = (
        odds_stream.writeStream.format("parquet")
        .option("path", odds_raw_path)
        .option("checkpointLocation", f"{args.hdfs_base}/checkpoints/odds_ticks_raw")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .outputMode("append")
        .start()
    )

    events_writer = (
        events_stream.writeStream.format("parquet")
        .option("path", events_raw_path)
        .option("checkpointLocation", f"{args.hdfs_base}/checkpoints/match_events_raw")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .outputMode("append")
        .start()
    )

    def process_batch(batch_df, batch_id):
        # batch_df is the heartbeat's own rows -- unused, it exists only to
        # get this function called on a fixed clock (see module docstring).
        # The three streaming queries in this job start independently and
        # race a little, so an early trigger here can hit "raw tables not
        # ready yet" -- harmless, since the heartbeat guarantees another
        # trigger regardless, unlike gating on the odds stream would.
        try:
            odds = spark.read.parquet(odds_raw_path)
            events = spark.read.parquet(events_raw_path)
        except Exception as exc:  # raw tables may not exist yet on the very first trigger(s)
            print(f"[divergence-job] batch {batch_id}: raw tables not ready yet ({exc})", flush=True)
            return

        result = score_divergence(odds, events)
        result.write.mode("overwrite").parquet(scores_path)
        flagged_count = result.filter(F.col("flagged")).count()
        print(f"[divergence-job] batch {batch_id}: scored {result.count()} windows, {flagged_count} flagged", flush=True)

    # Heartbeat, not a real data source -- see module docstring for why the
    # scoring trigger can't be the odds stream itself.
    heartbeat = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

    scoring_writer = (
        heartbeat.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"{args.hdfs_base}/checkpoints/divergence_scores")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
