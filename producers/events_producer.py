"""Kafka producer that replays a match's real event timeline from API-Football.

Publishes to KAFKA_TOPIC_EVENTS ("match-events"). Requires API_FOOTBALL_KEY in
.env -- see producers/README.md for how to get one (free tier, ~100 req/day).

Use the same --match-id here and in odds_producer.py so Spark can join the two
topics later. Team names differ between football-data.co.uk ("Man City") and
API-Football ("Manchester City"), so the canonical id can't be derived from
either source's team-name strings independently -- the API-Football fixture
id is a clean, source-independent choice; pass it explicitly to both.

Usage:
    python -m producers.events_producer --fixture-id 1035037 --match-id 1035037
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent
API_BASE_URL = "https://v3.football.api-sports.io"
CACHE_DIR = REPO_ROOT / "data" / "interim" / "events"


def _normalize_event_type(api_type: str, detail: str | None) -> str | None:
    """Maps an API-Football event type/detail to our internal type (keys into
    odds_interpolation.EVENT_WEIGHTS). Returns None for events we don't treat
    as market-moving (e.g. VAR review entries, which duplicate a Card/Goal
    entry that's already captured separately)."""
    api_type = api_type.lower()
    detail = (detail or "").lower()

    if api_type == "goal":
        return "Penalty Missed" if "missed" in detail else "Goal"
    if api_type == "card":
        if "red" in detail or "second yellow" in detail:
            return "Red Card"
        if "yellow" in detail:
            return "Yellow Card"
        return None
    if api_type == "subst":
        return "Substitution"
    return None


def fetch_raw_events(fixture_id: int, api_key: str) -> list[dict]:
    response = requests.get(
        f"{API_BASE_URL}/fixtures/events",
        headers={"x-apisports-key": api_key},
        params={"fixture": fixture_id},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("errors"):
        raise RuntimeError(f"API-Football error: {payload['errors']}")
    return payload["response"]


def normalize_events(raw_events: list[dict], duration: int = 90) -> list[dict]:
    """Stoppage-time events (e.g. a 90+4' card) are clipped to `duration` --
    our interpolation model replays exactly 90 ticks, so anything past full
    time lands on the final tick rather than being dropped."""
    normalized = []
    for e in raw_events:
        event_type = _normalize_event_type(e["type"], e.get("detail"))
        if event_type is None:
            continue
        elapsed = e["time"]["elapsed"] + (e["time"].get("extra") or 0)
        normalized.append(
            {
                "minute": min(elapsed, duration),
                "type": event_type,
                "team": e["team"]["name"],
                "player": e["player"]["name"] if e.get("player") else None,
            }
        )
    normalized.sort(key=lambda e: e["minute"])
    return normalized


def get_events(fixture_id: int, api_key: str, use_cache: bool = True) -> list[dict]:
    """Caches normalized events to data/interim/events/<fixture_id>.json (gitignored)
    so re-runs don't burn the free tier's ~100 req/day quota."""
    cache_path = CACHE_DIR / f"{fixture_id}.json"
    if use_cache and cache_path.exists():
        with open(cache_path) as f:
            return json.load(f)

    normalized = normalize_events(fetch_raw_events(fixture_id, api_key))

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(normalized, f, indent=2)

    return normalized


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def replay(match_id: str, events: list[dict], producer: KafkaProducer, topic: str, speed: float) -> None:
    last_minute = 0
    for event in events:
        if speed > 0:
            time.sleep((event["minute"] - last_minute) * speed)
        last_minute = event["minute"]

        message = {
            "match_id": match_id,
            "minute": event["minute"],
            "type": event["type"],
            "team": event["team"],
            "player": event["player"],
            "produced_at": datetime.now(timezone.utc).isoformat(),
        }
        producer.send(topic, key=match_id, value=message)
        print(f"[events-producer] minute {event['minute']:>2} -> {event['type']} ({event['team']})")
    producer.flush()


def main():
    parser = argparse.ArgumentParser(description="Replay a match's real event timeline onto Kafka.")
    parser.add_argument("--fixture-id", type=int, required=True, help="API-Football fixture id (see find_fixture.py).")
    parser.add_argument(
        "--match-id",
        default=None,
        help="Canonical match id shared with odds_producer.py (defaults to the fixture id).",
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_EVENTS", "match-events"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument(
        "--speed",
        type=float,
        default=0.2,
        help="Real seconds per simulated match minute (0 = publish as fast as possible). Match this to odds_producer's --speed to keep both topics in sync.",
    )
    parser.add_argument("--no-cache", action="store_true", help="Force a fresh API call instead of using the cached events file.")
    args = parser.parse_args()

    api_key = os.getenv("API_FOOTBALL_KEY")
    if not api_key:
        raise SystemExit("API_FOOTBALL_KEY not set -- copy .env.example to .env and fill it in.")

    match_id = args.match_id or str(args.fixture_id)
    events = get_events(args.fixture_id, api_key, use_cache=not args.no_cache)

    print(f"[events-producer] replaying fixture {args.fixture_id} as match_id '{match_id}' ({len(events)} events) -> topic '{args.topic}'")
    producer = build_producer(args.bootstrap_servers)
    replay(match_id, events, producer, args.topic, args.speed)
    print("[events-producer] done.")


if __name__ == "__main__":
    main()
