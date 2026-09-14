# producers/ — Role A (Ingestion / Streaming Engineer)

Two Kafka producer scripts that replay a match's real history at (simulated) real-time pace:

- `odds_producer.py` — reads a match's opening/closing odds from a football-data.co.uk CSV row, interpolates intra-match movement weighted toward event minutes, and publishes ticks to the `odds-ticks` topic.
- `events_producer.py` — reads a match's event timeline from API-Football (goals, cards, subs) and publishes each event to the `match-events` topic at its real match minute.
- `fetch_sample_data.py` — one-off script to pull and cache raw data from both sources into `data/raw/` (gitignored).

Both topics use the match id + kickoff timestamp so the Spark job can align them. See [PROJECT_PLAN.md](../PROJECT_PLAN.md) for the full architecture.
