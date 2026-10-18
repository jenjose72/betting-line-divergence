# Line Watch

Real-time betting-line divergence detection for football matches. Full idea, architecture, schedule, and rubric mapping: [PROJECT_PLAN.md](PROJECT_PLAN.md).

Pipeline: `Kafka (match-events + odds-ticks) → Spark Structured Streaming (windowed join + divergence scoring) → HDFS/Hive → Dashboard`

## Project layout

```
producers/    Role A — Kafka producers that replay events + odds, plus a synthetic validation case
streaming/    Role B — Spark Structured Streaming divergence-scoring job, Hive table/query SQL
dashboard/    Role C — Streamlit dashboard reading the job's output off HDFS
data/         gitignored — raw pulls from football-data.co.uk / API-Football land here
```

Each folder has its own README with design decisions and what's been verified. This file is the
step-by-step path to actually reproduce the whole thing from a clean machine.

## Prerequisites

- Docker + Docker Compose
- Python 3.12 (a different 3.x will likely work too, but this is what's been tested)
- A free API-Football key — sign up at [dashboard.api-football.com/register](https://dashboard.api-football.com/register) (Free plan, no card, ~100 requests/day)

---

## Reproducing this project, step by step

### 1. Clone and configure

```bash
git clone https://github.com/jenjose72/betting-line-divergence.git
cd betting-line-divergence
cp .env.example .env
```

Open `.env` and set `API_FOOTBALL_KEY` to the key from the prerequisites. Never commit `.env` —
it's already gitignored.

### 2. Bring up the stack

```bash
docker compose up -d
```

This starts 8 containers: ZooKeeper, Kafka, HDFS (namenode + datanode), Hive (metastore + its
Postgres DB + server), and a Spark standalone cluster (master + worker). First boot takes a few
minutes while Hadoop/Hive initialize.

Confirm everything's healthy before moving on:
```bash
docker compose ps --format 'table {{.Name}}\t{{.Status}}'
```
`namenode` and `datanode` should show `(healthy)`; the rest just need to be `Up`.

> **If Kafka exits on first boot** with a ZooKeeper connection error, that's a known transient
> race — the ephemeral node from a previous run hasn't expired yet. `docker compose up -d kafka`
> again clears it within a few seconds.
>
> **If you ever see `InconsistentClusterIdException`** after recreating the stack, ZooKeeper and
> Kafka's state have fallen out of sync (shouldn't happen — both have persistent volumes — but if
> it does): `docker compose rm -f -s kafka && docker volume rm betting-line-divergence_kafka_data && docker compose up -d kafka`.

Useful UIs once it's up: Kafka on `localhost:9092`, HDFS on `http://localhost:9870`, Spark master
on `http://localhost:8080`, Hive server on `localhost:10000`.

### 3. Python environment

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

> `requirements.txt` pins `pyspark==3.5.1` for local unit tests specifically — the Spark *cluster*
> containers run 3.1.1 (see `docker-compose.yml`). 3.1.1's bundled `cloudpickle` can't parse Python
> 3.12 bytecode, which only breaks local pytest runs, not the actual cluster job. Details in
> `requirements.txt`'s comment.

### 4. Fetch real data

```bash
python -m producers.fetch_odds_data --division E0 --season 2324
```

Pulls the real 2023/24 Premier League season CSV from football-data.co.uk (free, no signup) to
`data/raw/E0_2324.csv` — real Bet365 opening and closing odds.

### 5. Find a match's canonical id

```bash
python -m producers.find_fixture --team Burnley --date 2023-08-11
```

Prints the API-Football fixture id for that match (`1035037` for this one). You need this because
football-data.co.uk and API-Football spell team names differently ("Man City" vs "Manchester
City") — passing the same explicit `--match-id` to both producers below is what keeps the two
Kafka topics joinable by Spark later.

### 6. Replay a real match onto Kafka

```bash
python -m producers.events_producer --fixture-id 1035037 --match-id 1035037 --speed 0
python -m producers.odds_producer --home Burnley --away "Man City" --match-id 1035037 \
  --events data/interim/events/1035037.json --speed 0
```

The first publishes the real event timeline (goals, cards, subs) to `match-events`; the second
publishes 91 interpolated odds ticks (minute 0–90) to `odds-ticks`. `--speed 0` publishes as fast
as possible; use e.g. `--speed 0.1` for a visibly-ticking replay. Confirm messages landed:
```bash
docker exec linewatch-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic odds-ticks --from-beginning --max-messages 5 --property print.key=true
```

### 7. Replay the synthetic validation case

```bash
python -m producers.synthetic_leak_demo --speed 0
```

Publishes one deliberately-constructed test case (`match_id="SYNTH-LEAK-DEMO"`, non-numeric so it
can't be mistaken for real data) to both topics: odds crash sharply with no explaining event, then
the goal that would explain a move that size arrives 11 minutes later. Exists to prove the
detector can actually flag something, not just stay silent on real matches — see
`producers/README.md` and `streaming/README.md` for why.

### 8. Run the Spark Structured Streaming job

```bash
docker exec linewatch-spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
  /workspace/streaming/divergence_job.py \
  --bootstrap-servers kafka:29092
```

Runs continuously — three streaming queries (two landing raw Kafka data in HDFS as Parquet, one
recomputing the windowed join + divergence score every 10s). Leave it running; it rescores
automatically as new data arrives, no need to restart it per match. Watch its own log line
(`[divergence-job] batch N: scored X windows, Y flagged`) to confirm it's picking up what you
published in steps 6–7. First trigger or two may say "raw tables not ready yet" — that's an
expected startup race, it self-corrects.

### 9. Create the Hive tables and run analysis queries

Once the job has produced at least one batch:
```bash
cat streaming/hive_tables.sql | docker exec -i linewatch-hive-server beeline -u jdbc:hive2://localhost:10000
docker exec -i linewatch-hive-server beeline -u jdbc:hive2://localhost:10000 < streaming/hive_queries.sql
```
The first creates three external tables (`odds_ticks_raw`, `match_events_raw`,
`divergence_scores`) over the HDFS paths Spark just wrote — Hive never talks to Spark directly,
only through those shared paths (see `streaming/README.md` for why). The second runs a few
ready-made analysis queries (row counts, flagged windows, etc.) against them.

### 10. View the dashboard

```bash
streamlit run dashboard/app.py
```
Open `http://localhost:8501`. Select a match in the sidebar — you should see its odds lines,
event markers, and the divergence score per window, red where flagged. If it says "No data yet,"
step 8's Spark job hasn't produced a scored batch yet; give it a few more seconds.

### 11. Run the test suite

```bash
pytest
```
23 tests across `producers/tests/` and `streaming/tests/` — interpolation math, real-API event
normalization, the synthetic case's shape, and the divergence scoring logic (including the
left-join and input-validation edge cases). All should pass with no Docker stack required for
this step — they run against local data/fixtures and a local Spark session.

---

## Troubleshooting

- **A command times out or can't connect to Kafka, but `docker compose ps` looks fine.** Check for
  a port collision with something else on the machine: `docker ps -a --filter "publish=9092"`
  should show only `linewatch-kafka`. A different Kafka broker squatting on the same host port
  will accept the TCP connection — so it doesn't look like a dead port — and then every real
  produce/consume silently times out, because you're talking to the wrong cluster. If something
  else owns 9092, either stop it or change this project's host port mapping (and the matching
  `KAFKA_ADVERTISED_LISTENERS` entry) in `docker-compose.yml` together, consistently — the two
  can't be out of sync or you get the same failure from inside your own stack.
- **Kafka container keeps exiting right after `docker compose up`.** See the ZooKeeper race note
  in step 2 — just retry `docker compose up -d kafka`.
- **Dashboard shows "No data yet."** The Spark job (step 8) hasn't scored a batch yet, or isn't
  running. It needs to have processed at least one trigger before HDFS has anything for the
  dashboard to read.

## Branch / PR workflow

- No direct commits to `main` — it's branch-protected; every change goes through a PR.
- Branch names: `feat/<short-desc>`, `fix/<short-desc>`, `chore/<short-desc>`, or `docs/<short-desc>`.
- Open a PR against `main` with `gh pr create` (or the GitHub UI) once the branch is pushed. Small,
  reviewable PRs over one giant one.
- At least one other teammate reviews/approves before merge — that's also what gives you real
  material for the "individual contribution breakdown" in the final report.
- Squash or regular merge is fine; just keep `main` always in a working state.

## Status

Phases 0–3 (environment, both producers, the streaming job, the dashboard) are done and verified
end to end against the live stack, including a real synthetic-leak detection. Next: Phase 4 (final
report + packaging). See [PROJECT_PLAN.md](PROJECT_PLAN.md) for the full schedule.
