# streaming/ — Role B (Analytics / Spark Engineer)

`divergence_job.py` — the Spark Structured Streaming job. Reads both `match-events` and `odds-ticks` from Kafka, writes raw parsed data to HDFS as Parquet, and on a fixed clock (independent of whether new Kafka data has arrived — see below) recomputes a windowed join scoring how far each 5-match-minute window's actual odds movement diverges from what its events would explain. Results land in a Hive external table (`hive_tables.sql`).

## The model

For each match_id × 5-minute window:
- **actual_move** — the largest swing in implied win probability (home/draw/away, de-vigged) across the ticks in that window.
- **expected_move** — a small constant background drift (0.01, "the market isn't perfectly still even absent news") plus a per-event-type prior (Goal 0.12, Red Card 0.06, Yellow Card 0.02, Substitution 0.01, ...), summed over whatever events fell in that window.
- **divergence_score** = actual_move − expected_move. Flagged when this exceeds 0.05 — i.e. the market moved meaningfully more than its own events explain.

**Why the expected-move priors are a separate table from `producers/odds_interpolation.EVENT_WEIGHTS`, not the same one reused:** the producer's table shapes how the simulated "actual" odds move in the first place. If Spark's notion of "expected" reused that exact table, actual and expected would be mathematically tied to the same generating process and divergence would collapse to ~0 everywhere by construction — an analysis that can't fail isn't an analysis. This job's `EVENT_SHIFTS` is an independently-set prior for what a goal *should* move an efficient market, expressed directly in probability units.

**Why Hive tables aren't created via Spark's Hive catalog (`enableHiveSupport`):** that couples Spark's bundled Hive client version to the Hive server's (2.3.2) — exactly the kind of version mismatch that broke Kafka and `kafka-python` earlier in this project. Instead, Spark writes plain Parquet to fixed HDFS paths; `hive_tables.sql` creates external tables over those same paths once, separately. Neither side needs to know about the other beyond the shared path.

**Why each trigger recomputes the whole windowed join from scratch instead of a true incremental stream-stream join:** Spark 3.1's stream-stream outer joins are the hardest variant to get right, and a left-outer join is exactly what's needed here (a window with odds movement but *zero* matching events — the single most important case for this analysis — must still show up, or the whole point of the tool is lost). At this project's data scale (a handful of matches, ~100–200 rows each), a full recompute per trigger is cheap and sidesteps an entire class of streaming-state bugs.

**Why the recompute is driven by a `rate`-source heartbeat, not the odds stream itself** — found by testing, not by inspection: Structured Streaming's `ProcessingTime` trigger only fires a new micro-batch when its *source* has new offsets, it doesn't run on a real clock the way the name implies. Replaying a match at `--speed 0` (or any replay that finishes) delivers all of a topic's data in one micro-batch; after that there are no new offsets, so nothing would ever call `process_batch` again — the score table would freeze forever at whatever partial state existed when raw data first became readable. A built-in `rate` source (1 row/sec, rows never touched) exists purely as a clock, so the recompute fires reliably every trigger interval regardless of whether the real topics have new data.

## Running it

```bash
# one-time, after the job has produced at least one Hive-external-table-shaped output:
cat streaming/hive_tables.sql | docker exec -i linewatch-hive-server beeline -u jdbc:hive2://localhost:10000

# the job itself (from the host, once producers have published data to replay):
docker exec linewatch-spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
  /workspace/streaming/divergence_job.py \
  --bootstrap-servers kafka:29092
```

## Verified

Ran end to end against the live cluster with the real Burnley 0-3 Man City match (fixture `1035037`): all 19 windows (0–90 in steps of 5) correctly joined against the real event timeline — e.g. window 0 shows the real minute-4 Haaland goal, window 90 shows the real stoppage-time red card plus 3 subs, matching `events_producer.py`'s verified output exactly. Confirmed both via a direct Parquet read off HDFS and via real Hive SQL through `beeline` (91 rows in `odds_ticks_raw`, 14 in `match_events_raw`, 19 scored windows in `divergence_scores`).

**Honest result, not a cherry-picked one:** none of the 19 windows are flagged for this match. That's a real, explainable finding, not a bug — Burnley opened at 8.0 (≈12% implied win probability), already such a lopsided line that there was very little room left for that probability to move further even across 3 goals (it moved from ≈11.8% to ≈10.6% over the *entire* match). A second real match (Tottenham 2-0 Man United, closer pre-match odds) also shows 0 flagged windows — see `dashboard/README.md` for why: interpolating between real opening/closing lines produces structurally smoother movement than a true tick feed would, so local volatility around events is inherently underrepresented. That's a disclosed limitation of the data source, not evidence the detector is broken.

**Proof it isn't broken — a true-positive case:** `producers/synthetic_leak_demo.py` publishes one deliberately-constructed test case (`match_id="SYNTH-LEAK-DEMO"`, clearly non-numeric, never confused with real data): odds crash sharply with zero events anywhere nearby, then the goal that would explain a move that size happens 11 minutes later, once it's already fully priced in. Run through this exact job: **window 35 flagged** (actual_move 0.194 vs. expected 0.01, divergence +0.184) — the leak, caught. **Window 50 — the window containing the actual goal — correctly stays unflagged** (actual_move only 0.003 vs. expected 0.13, since the market had nothing left to react to). That second detail matters as much as the flag itself: the tool isn't just a big-move alarm, it's specifically scoring moves against the news available *at that time*, and correctly recognizes that a real goal isn't surprising if the market already knew. Confirmed both via direct HDFS Parquet read and real Hive SQL via beeline.

Also found and fixed two real bugs along the way (see commit history / module docstring): an early-trigger race against the raw-table writers, and — the more fundamental one — `ProcessingTime` triggers not firing at all once a replay's data has all arrived in one micro-batch, which would have silently frozen the score table forever.

See [PROJECT_PLAN.md](../PROJECT_PLAN.md) for the full architecture. Phase 2 (streaming analytics) is done — next is the dashboard (Phase 3).
