# Demo script — Jen / Ingestion (Kafka)

~7 minutes. Narration first, commands and screens as separate reference sections at the bottom so you
can keep this open and glance down at exactly what to type/click as you talk.

---

## What to say

### 1. The problem we're solving (~30s)

> Betting odds are supposed to move only when public information changes — a goal, a red card, a
> confirmed injury. Those are legitimate reasons for the market to reprice. But if the odds move
> *before* any of that happens, or move by *more* than the event justifies, that gap is suspicious —
> it can mean leaked information, insider knowledge, or a bookmaker reacting to unusual betting
> volume. That's a real pattern integrity analysts look for manually. We built a system that looks
> for it automatically.

### 2. What we built — the whole project (~60s)

> We call it Line Watch. It replays a football match's real timeline — goals, cards, subs — through
> one Kafka topic, and that same match's real odds movement through a second Kafka topic, at the
> same time. A Spark Structured Streaming job joins both topics in 5-minute windows, computes how
> much the odds *actually* moved versus how much they were *expected* to move given the events in
> that window, and scores the gap. Results land in HDFS and a Hive table, and a Streamlit dashboard
> shows both lines ticking with divergence flags lighting up in real time.
>
> It genuinely needs all three tools — Kafka has to carry two independently-timed live feeds, Spark
> has to join and score them continuously as they arrive, and HDFS/Hive persist and serve the result.
>
> And the honest result: two real Premier League matches we replayed show zero flagged windows —
> nothing unusual happened in either, which is itself the correct answer. To prove the detector
> actually *works* and isn't just silent, we also built one deliberately constructed test case with
> a real leak pattern injected into it — and it gets flagged, in exactly the right window.

### 3. What I did (~20s)

> My part of the project is the ingestion layer — getting real data in and onto Kafka correctly.
> That's everything in `producers/`: pulling the real datasets, building the model that turns
> opening/closing odds into a minute-by-minute stream, and the two Kafka producers that actually
> publish it.

### 4. My part, in detail — walk through it live (~4.5 min)

> **Real data, not a toy dataset.** This pulls the real Premier League 2023/24 season — Bet365
> opening and closing odds, free, no signup needed.
> *(run `fetch_odds_data.py`, open the CSV, point at `B365H` / `B365CH`)*

> **A real integration bug I had to solve.** football-data.co.uk calls this team "Man City."
> API-Football calls it "Manchester City." Two different data sources, two different spellings for
> the same team. If each producer built its Kafka key from its own source's team-name spelling, the
> two topics would carry different keys for the same match, and Spark's join would silently find
> nothing — no error, just an empty result. So both producers take an explicit shared `--match-id`:
> the API-Football fixture id, which is source-independent.
> *(run `find_fixture.py`, say the id out loud)*

> **The odds model.** football-data.co.uk only gives opening and closing odds, not live ticks. So I
> built a model that interpolates between them — 30% slow background drift, 70% event-weighted
> jumps, smoothed around each event's minute, done in de-vigged probability space since that's what
> events actually change. That's a stated modeling assumption, not a hidden gap — I say that
> up front rather than have it found later.
> *(open `odds_interpolation.py`, point at `interpolate_odds()` for a few seconds, don't read code
> line by line)*

> **Now the real thing: publish both streams and watch them land.**
> *(run `events_producer.py`, then `odds_producer.py` with a small `--speed` so it visibly ticks,
> then in the second pane run `kafka-console-consumer` and point at the key — same `match_id` on
> both topics, which is the fix from a minute ago, working)*

> **And the validation case.** A detector that never flags anything looks identical to a broken one.
> So I built one deliberately constructed test case — odds crash sharply with zero events anywhere
> near it, and the goal that would explain a move that size happens 11 minutes later, once it's
> already priced in. That's the actual signature this whole project is trying to catch.
> *(run `synthetic_leak_demo.py`)*

### 5. Proof it's real beyond just my part (~45s)

> This isn't a demo that stops at Kafka — here's the data actually landing downstream.
> *(show offset counts via `GetOffsetShell`, then a quick cut to the dashboard with the lines you
> just published, ticking)*

### 6. Close (~20s)

> One thing I'd flag myself: the odds are modeled from real opening/closing lines, not true live
> tick data — that's the main limitation, and it's why our two real matches show no flags while a
> real in-play feed might show more local volatility. Also, Kafka needed two topics rather than one
> because the two feeds arrive on completely independent schedules — odds every match-minute, events
> only when something happens.

---

## Commands to run, in order

```bash
# pre-flight, off-recording — confirm the stack is healthy before you start
docker compose ps --format 'table {{.Name}}\t{{.Status}}'
docker exec linewatch-namenode hdfs dfs -rm -r -f /data/line_watch   # clear old demo data

# ALSO check for a port collision before you start -- if another docker-compose
# project on the machine (e.g. a teammate's "streaming" stack) is using 8080 or
# 9092, this command will *appear* to connect but every real produce/consume
# will silently time out, because you're actually talking to their broker:
docker ps -a --filter "publish=9092"   # should show nothing but linewatch-kafka
docker ps -a --filter "publish=8080"   # should show nothing but linewatch-spark-master
# if either shows someone else's container, coordinate who's running their
# stack when, or temporarily remap your own ports (ask me, I've done this live)

# section 4a — real data
python -m producers.fetch_odds_data --division E0 --season 2324

# section 4b — the match_id problem
python -m producers.find_fixture --team Burnley --date 2023-08-11

# section 4c — open producers/odds_interpolation.py in your editor, talk over it (no command)

# section 4d — run both producers (separate panes), then consume
python -m producers.events_producer --fixture-id 1035037 --match-id 1035037 --speed 0.05
python -m producers.odds_producer --home Burnley --away "Man City" --match-id 1035037 \
  --events data/interim/events/1035037.json --speed 0.05
docker exec linewatch-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic odds-ticks --from-beginning --max-messages 5 --property print.key=true

# section 4e — the validation case
python -m producers.synthetic_leak_demo --speed 0

# section 5 — proof it's real
docker exec linewatch-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic odds-ticks
```

## Pages / screens to show

- **Your editor** — `producers/odds_interpolation.py` (section 4c), `producers/odds_producer.py`
  briefly if asked about `--match-id`.
- **Terminal, two panes** — producers running (left) and `kafka-console-consumer` output (right),
  for section 4d. This is the single most important visual in your section: real keys, real data.
- **Streamlit dashboard** (`streamlit run dashboard/app.py`, `localhost:8501`) — select your match
  in the sidebar, point at the odds lines ticking and the event markers lining up with them. This is
  the payoff shot for section 5.
  **Dependency, confirmed by testing this script end to end:** the dashboard reads from HDFS, which
  is only populated once `streaming/divergence_job.py` (Praneetha's part) has actually run and
  scored at least one batch — without it the dashboard correctly shows "No data yet," which is fine
  as a fallback line but isn't the shot you want. Either coordinate so the Spark job is already
  running before your segment, or start it yourself a minute or two before you get to this part:
  `docker exec linewatch-spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 /workspace/streaming/divergence_job.py --bootstrap-servers kafka:29092`
  (give it ~20-30s after your producers finish before it has something to show).
- **Optional, if time allows** — Spark master UI (`localhost:8080`) showing the running job, or the
  HDFS UI (`localhost:9870`) showing `/data/line_watch/odds_ticks_raw` with real files in it. Only
  include these if Praneetha/Rupsika's parts of the demo don't already cover them — no need to
  duplicate.
