# Line Watch

Real-time betting-line divergence detection for football matches. Full idea, architecture, schedule, and rubric mapping: [PROJECT_PLAN.md](PROJECT_PLAN.md).

Pipeline: `Kafka (match-events + odds-ticks) → Spark Structured Streaming (windowed join + divergence scoring) → Hive/HDFS → Dashboard`

## Project layout

```
producers/    Role A — Kafka producers that replay events + odds
streaming/    Role B — Spark Structured Streaming divergence-scoring job
dashboard/    Role C — live dashboard reading from Hive
data/         gitignored — raw pulls from football-data.co.uk / API-Football land here
```

Each folder has its own README with specifics.

## Local setup

1. Copy `.env.example` → `.env` and fill in your API-Football key (get one free at api-sports.io).
2. Bring up the stack:
   ```bash
   docker compose up -d
   ```
   This starts Kafka, HDFS (namenode/datanode), Hive (metastore + server), and a Spark standalone cluster (master + worker). First boot takes a few minutes while Hadoop/Hive initialize.

   > This compose file hasn't been smoke-tested yet (scaffolded without Docker available). If a container fails to come up cleanly, fix it forward on your branch and note what changed in the PR.
3. Create a Python virtualenv and install dependencies:
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```
4. Useful UIs once the stack is up: Kafka on `localhost:9092`, HDFS on `http://localhost:9870`, Spark master on `http://localhost:8080`, Hive server on `localhost:10000`.

## Branch / PR workflow

- No direct commits to `main`. Every change goes on a branch: `feat/<short-desc>`, `fix/<short-desc>`, or `chore/<short-desc>`.
- Open a PR against `main` with `gh pr create` (or the GitHub UI) once the branch is pushed. Small, reviewable PRs over one giant one — each phase in the plan should be a small handful of PRs, not one.
- At least one other teammate reviews/approves before merge, even on a 3-person team — that's also what gives you real material for the "individual contribution breakdown" in the final report.
- Squash or regular merge is fine; just keep `main` always in a working state.

## Status

Phase 0 (environment scaffolding) in progress — see [PROJECT_PLAN.md](PROJECT_PLAN.md) for the full 2-week schedule.
