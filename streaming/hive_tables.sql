-- Run once, after divergence_job.py has been running for at least one
-- trigger interval (so the HDFS paths below actually exist). Hive external
-- tables just read whatever Parquet files sit at LOCATION -- Spark and Hive
-- never talk to each other directly, only through these shared paths.
--
-- Run from the host:
--   cat streaming/hive_tables.sql | docker exec -i linewatch-hive-server beeline -u jdbc:hive2://localhost:10000

CREATE EXTERNAL TABLE IF NOT EXISTS odds_ticks_raw (
    match_id STRING,
    minute INT,
    home_odds DOUBLE,
    draw_odds DOUBLE,
    away_odds DOUBLE,
    produced_at STRING,
    window_start INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/data/line_watch/odds_ticks_raw';

CREATE EXTERNAL TABLE IF NOT EXISTS match_events_raw (
    match_id STRING,
    minute INT,
    type STRING,
    team STRING,
    player STRING,
    produced_at STRING,
    window_start INT,
    expected_shift DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/data/line_watch/match_events_raw';

CREATE EXTERNAL TABLE IF NOT EXISTS divergence_scores (
    match_id STRING,
    window_start INT,
    n_ticks BIGINT,
    n_events BIGINT,
    event_types STRING,
    actual_move DOUBLE,
    expected_move DOUBLE,
    divergence_score DOUBLE,
    flagged BOOLEAN
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/data/line_watch/divergence_scores';
