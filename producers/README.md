# producers/ — Role A (Ingestion / Streaming Engineer)

- `fetch_odds_data.py` — pulls a season's real odds CSV from football-data.co.uk (no signup) into `data/raw/`.
- `odds_interpolation.py` — pure function: given a match's opening/closing odds and its event timeline, models intra-match odds movement (de-vigged probability interpolation, weighted toward event minutes). Unit tested in `tests/test_odds_interpolation.py`.
- `odds_producer.py` — CLI that loads a match's real odds row, interpolates it against an event timeline, and replays it minute-by-minute onto the `odds-ticks` Kafka topic.
- `fixtures/sample_match_events.json` — a **synthetic** event timeline used until `events_producer.py` (next branch) supplies real minute-by-minute events from API-Football.

## Running it

```bash
# one-time: pull a real season's odds
python -m producers.fetch_odds_data --division E0 --season 2324

# replay one match's odds onto Kafka (as fast as possible)
python -m producers.odds_producer --home Burnley --away "Man City" --speed 0

# or at ~5 simulated minutes/second for a live-feeling demo
python -m producers.odds_producer --home Burnley --away "Man City" --speed 0.2
```

Each tick is a JSON message keyed by `match_id`:
```json
{"match_id": "Burnley_ManCity_2023-08-11", "minute": 23, "home_odds": 8.34, "draw_odds": 5.38, "away_odds": 1.33, "produced_at": "..."}
```

## Verified

Run end to end against the live Kafka container: real opening/closing odds pulled from football-data.co.uk, 91 ticks (minute 0–90) published and confirmed landed in the `odds-ticks` topic via `kafka-console-consumer`, minute 0 exactly matches the CSV's opening odds and minute 90 exactly matches its closing odds, with visible acceleration around each event minute in the fixture.

`events_producer.py` (real API-Football event timelines) is the next branch — see [PROJECT_PLAN.md](../PROJECT_PLAN.md) for the full architecture and Phase 1 scope.
