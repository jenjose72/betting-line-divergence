-- Line Watch: Hive analysis queries
-- Run after streaming/hive_tables.sql has created the external tables.
--
-- From the project root:
--   cat streaming/hive_queries.sql | docker exec -i linewatch-hive-server beeline -u jdbc:hive2://localhost:10000

-- 1. Row counts in each Hive external table
SELECT 'odds_ticks_raw' AS table_name, COUNT(*) AS row_count
FROM odds_ticks_raw;

SELECT 'match_events_raw' AS table_name, COUNT(*) AS row_count
FROM match_events_raw;

SELECT 'divergence_scores' AS table_name, COUNT(*) AS row_count
FROM divergence_scores;


-- 2. Number of matches represented in each table
SELECT 'odds_ticks_raw' AS table_name, COUNT(DISTINCT match_id) AS match_count
FROM odds_ticks_raw;

SELECT 'match_events_raw' AS table_name, COUNT(DISTINCT match_id) AS match_count
FROM match_events_raw;

SELECT 'divergence_scores' AS table_name, COUNT(DISTINCT match_id) AS match_count
FROM divergence_scores;


-- 3. Divergence windows that were flagged
SELECT
    match_id,
    window_start,
    n_ticks,
    n_events,
    event_types,
    actual_move,
    expected_move,
    divergence_score
FROM divergence_scores
WHERE flagged = TRUE
ORDER BY divergence_score DESC;


-- 4. Highest-divergence windows, regardless of whether they were flagged
SELECT
    match_id,
    window_start,
    n_events,
    event_types,
    actual_move,
    expected_move,
    divergence_score,
    flagged
FROM divergence_scores
ORDER BY divergence_score DESC
LIMIT 10;


-- 5. Windows where the market moved but no event was recorded
SELECT
    match_id,
    window_start,
    actual_move,
    expected_move,
    divergence_score,
    flagged
FROM divergence_scores
WHERE n_events = 0
  AND actual_move > 0
ORDER BY divergence_score DESC;


-- 6. Event counts by event type
SELECT
    type,
    COUNT(*) AS event_count
FROM match_events_raw
WHERE type IS NOT NULL
GROUP BY type
ORDER BY event_count DESC;


-- 7. Per-match divergence summary
-- Useful for comparing matches in the final analysis/report.
SELECT
    match_id,
    COUNT(*) AS windows,
    SUM(CASE WHEN flagged = TRUE THEN 1 ELSE 0 END) AS flagged_windows,
    MAX(divergence_score) AS max_divergence,
    AVG(divergence_score) AS avg_divergence
FROM divergence_scores
GROUP BY match_id
ORDER BY max_divergence DESC;

