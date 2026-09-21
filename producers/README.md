# producers/ — Role A (Ingestion / Streaming Engineer)

- `fetch_odds_data.py` — pulls a season's real odds CSV from football-data.co.uk (no signup) into `data/raw/`.
- `odds_interpolation.py` — pure function: given a match's opening/closing odds and its event timeline, models intra-match odds movement (de-vigged probability interpolation, weighted toward event minutes). Unit tested in `tests/test_odds_interpolation.py`.
- `odds_producer.py` — CLI that loads a match's real odds row, interpolates it against an event timeline, and replays it minute-by-minute onto the `odds-ticks` Kafka topic.
- `find_fixture.py` — looks up an API-Football fixture id by team + date.
- `events_producer.py` — CLI that fetches a match's real event timeline from API-Football (caching to `data/interim/events/`, gitignored, to stay well under the free tier's ~100 req/day) and replays it onto the `match-events` Kafka topic. Unit tested against a real captured API response in `tests/test_events_normalization.py`.
- `fixtures/sample_match_events.json` — a **synthetic** event timeline, now only used as `odds_producer.py`'s default when `--events` isn't overridden; prefer real events from `events_producer.py`/`find_fixture.py` instead.
- `synthetic_leak_demo.py` — publishes one deliberate, clearly-labeled synthetic test case (`match_id="SYNTH-LEAK-DEMO"`, non-numeric so it can never be mistaken for a real fixture): odds crash sharply with no explaining event, then the goal that would explain a move that size happens 11 minutes later. Validates that `divergence_job.py` actually flags the pattern this whole project exists to catch — see its module docstring for the full reasoning.

## Getting an API-Football key

Free tier, no credit card: sign up at [dashboard.api-football.com/register](https://dashboard.api-football.com/register) (or via the "API-FOOTBALL" listing on rapidapi.com if that portal gives you trouble), then:

```bash
cp .env.example .env
# edit .env and set API_FOOTBALL_KEY=your_key_here
```

`.env` is gitignored — never commit it or paste the key anywhere shared.

## Canonical match_id — read this before running both producers

football-data.co.uk and API-Football spell team names differently (**"Man City"** vs **"Manchester City"**), so each producer deriving a match_id independently from its own source's team-name string would produce two different ids for the same match — silently breaking Spark's windowed join later. Fix: pass the same explicit `--match-id` to both producers. The API-Football fixture id is a good canonical choice since it's source-independent.

## Running it

```bash
# one-time: pull a real season's odds
python -m producers.fetch_odds_data --division E0 --season 2324

# find the API-Football fixture id for a match
python -m producers.find_fixture --team Burnley --date 2023-08-11

# replay both topics for the same match under the same canonical id
python -m producers.events_producer --fixture-id 1035037 --match-id 1035037 --speed 0
python -m producers.odds_producer --home Burnley --away "Man City" --match-id 1035037 --speed 0
```

Drop `--speed 0` (or set `--speed 0.2` on both) for a slower, live-feeling replay — keep the same speed on both producers so the two topics stay roughly in sync minute-for-minute.

## Verified

Ran both producers end to end against the live Kafka containers for the real Burnley 0-3 Man City match (fixture 1035037, 2023-08-11):
- `match-events`: 14 real events (3 goals, 1 red card, 10 subs — API's 1 VAR entry correctly dropped as non-market-moving), first goal at minute 4 (Haaland) matching the real match
- `odds-ticks`: 91 ticks, minute 0 exactly matches the real opening odds, minute 90 exactly matches the real closing odds
- Both topics confirmed carrying the same key (`1035037`) via `kafka-console-consumer`, resolving the team-name-mismatch problem described above
- 10 unit tests passing (5 interpolation, 5 event normalization against a real captured API response)

See [PROJECT_PLAN.md](../PROJECT_PLAN.md) for the full architecture. Ingestion stage (Phase 1) is now complete — next up is the Spark Structured Streaming job (Phase 2).
