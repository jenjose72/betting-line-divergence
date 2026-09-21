"""Kafka producer that replays one match's odds as a sequence of interpolated ticks.

Publishes to KAFKA_TOPIC_ODDS. The event timeline used to shape the
interpolation is currently a synthetic test fixture
(producers/fixtures/sample_match_events.json) -- events_producer.py, a later
branch, will supply real minute-by-minute events from API-Football and this
script will take that as --events instead.

Usage:
    python -m producers.odds_producer --home Burnley --away "Man City"
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer

from producers.odds_interpolation import interpolate_odds

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_EVENTS_FIXTURE = Path(__file__).parent / "fixtures" / "sample_match_events.json"


def load_match_row(csv_path: Path, home_team: str, away_team: str) -> pd.Series:
    df = pd.read_csv(csv_path)
    match = df[(df["HomeTeam"] == home_team) & (df["AwayTeam"] == away_team)]
    if match.empty:
        raise ValueError(f"No match found for {home_team} vs {away_team} in {csv_path}")
    return match.iloc[0]


def make_match_id(row: pd.Series) -> str:
    date = pd.to_datetime(row["Date"], dayfirst=True).strftime("%Y-%m-%d")
    slug = lambda s: str(s).replace(" ", "").replace("'", "")
    return f"{slug(row['HomeTeam'])}_{slug(row['AwayTeam'])}_{date}"


def load_events(events_path: Path) -> list[dict]:
    with open(events_path) as f:
        data = json.load(f)
    return data["events"] if isinstance(data, dict) else data


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def replay(match_id: str, ticks: list[dict], producer: KafkaProducer, topic: str, speed: float) -> None:
    for tick in ticks:
        message = {
            "match_id": match_id,
            "minute": tick["minute"],
            "home_odds": tick["home_odds"],
            "draw_odds": tick["draw_odds"],
            "away_odds": tick["away_odds"],
            "produced_at": datetime.now(timezone.utc).isoformat(),
        }
        producer.send(topic, key=match_id, value=message)
        print(
            f"[odds-producer] minute {tick['minute']:>2} -> "
            f"H {tick['home_odds']} D {tick['draw_odds']} A {tick['away_odds']}"
        )
        if speed > 0:
            time.sleep(speed)
    producer.flush()


def main():
    parser = argparse.ArgumentParser(description="Replay a match's odds movement onto Kafka.")
    parser.add_argument("--csv", default=str(REPO_ROOT / "data/raw/E0_2324.csv"))
    parser.add_argument("--home", required=True)
    parser.add_argument("--away", required=True)
    parser.add_argument(
        "--match-id",
        default=None,
        help=(
            "Canonical match id to publish under, shared with events_producer.py. "
            "Team names differ between football-data.co.uk and API-Football (e.g. "
            "'Man City' vs 'Manchester City'), so the two producers can't derive a "
            "matching id independently -- pass the same explicit id to both (e.g. "
            "the API-Football fixture id). Defaults to a slug derived from this "
            "CSV row when omitted, for standalone use."
        ),
    )
    parser.add_argument("--events", default=str(DEFAULT_EVENTS_FIXTURE))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_ODDS", "odds-ticks"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument(
        "--speed",
        type=float,
        default=0.2,
        help="Real seconds per simulated match minute (0 = publish as fast as possible).",
    )
    args = parser.parse_args()

    row = load_match_row(Path(args.csv), args.home, args.away)
    match_id = args.match_id or make_match_id(row)
    open_odds = (float(row["B365H"]), float(row["B365D"]), float(row["B365A"]))
    close_odds = (float(row["B365CH"]), float(row["B365CD"]), float(row["B365CA"]))
    events = load_events(Path(args.events))

    ticks = interpolate_odds(open_odds, close_odds, events)

    print(f"[odds-producer] replaying {match_id} ({len(ticks)} ticks) -> topic '{args.topic}'")
    producer = build_producer(args.bootstrap_servers)
    replay(match_id, ticks, producer, args.topic, args.speed)
    print("[odds-producer] done.")


if __name__ == "__main__":
    main()
