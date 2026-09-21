"""Publishes ONE synthetic, clearly-labeled test case: a classic "leaked
information" pattern, to validate that divergence_job.py actually catches
what this whole project exists to catch.

This is NOT real match data and is never presented as such -- match_id is
"SYNTH-LEAK-DEMO" (non-numeric, unlike every real API-Football fixture id)
so it can never be confused with a real match in the dashboard or reports.

Why this exists: the two real matches replayed so far (Burnley/Man City,
Tottenham/Man United) both correctly show *no* flagged windows -- an honest
result, not a bug (see dashboard/README.md), but on its own it only proves
the detector doesn't cry wolf. It doesn't prove the detector can catch a
real anomaly, because neither real match happens to contain one. A
detector that never fires on anything is indistinguishable from a detector
that's silently broken. This constructs one deliberate positive case --
analogous to a red-team test or a known-bad fixture in a test suite -- so
both failure modes (false positives AND false negatives) are checked.

The scenario:
  - minutes  0-34: a flat, ordinary market (no news, no reason to move)
  - minutes 35-39: home odds crash from 3.0 to 1.8 in a single 5-minute
    window -- a huge, sharp move with ZERO events anywhere near it. This
    is the actual signature this project is about: the market moving on
    information nobody public has yet.
  - minutes 40-49: the market holds at the new (leaked) level -- it was
    already fully priced in, so there's nothing left to move
  - minute  50: the goal that "explains" the move actually happens here,
    11 minutes after the market already moved. Odds barely react, because
    there's no surprise left by the time the news is public. This window
    is expected to stay UNFLAGGED despite containing a real Goal event --
    the interesting, honest secondary finding: the tool doesn't just flag
    big moves, it correctly leaves alone a big *event* that wasn't itself
    surprising, because the market had already reacted to it in advance.
  - minutes 51-90: an ordinary gentle drift into the real closing line

Usage:
    python -m producers.synthetic_leak_demo --speed 0
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

MATCH_ID = "SYNTH-LEAK-DEMO"

# (home_odds, draw_odds, away_odds) at the boundaries of each segment.
QUIET_MARKET = (3.0, 3.4, 2.5)
POST_LEAK = (1.8, 3.6, 4.2)
FINAL_CLOSE = (1.7, 3.75, 4.6)

EVENTS = [{"minute": 50, "type": "Goal", "team": "Home", "player": "Demo Player"}]


def _lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def _lerp_odds(start, end, t):
    return tuple(round(_lerp(s, e, t), 3) for s, e in zip(start, end))


def build_ticks() -> list[dict]:
    ticks = []
    for minute in range(0, 91):
        if minute <= 34:
            odds = QUIET_MARKET
        elif minute <= 39:
            t = (minute - 35) / 4  # 0 at minute 35, 1 at minute 39
            odds = _lerp_odds(QUIET_MARKET, POST_LEAK, t)
        elif minute <= 50:
            odds = POST_LEAK
        else:
            t = (minute - 50) / 40  # 0 at minute 50, 1 at minute 90
            odds = _lerp_odds(POST_LEAK, FINAL_CLOSE, t)

        ticks.append({"minute": minute, "home_odds": odds[0], "draw_odds": odds[1], "away_odds": odds[2]})
    return ticks


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def publish_odds(producer: KafkaProducer, topic: str, speed: float):
    for tick in build_ticks():
        message = {"match_id": MATCH_ID, **tick, "produced_at": datetime.now(timezone.utc).isoformat()}
        producer.send(topic, key=MATCH_ID, value=message)
        if speed > 0:
            time.sleep(speed)
    producer.flush()


def publish_events(producer: KafkaProducer, topic: str):
    for event in EVENTS:
        message = {"match_id": MATCH_ID, **event, "produced_at": datetime.now(timezone.utc).isoformat()}
        producer.send(topic, key=MATCH_ID, value=message)
    producer.flush()


def main():
    parser = argparse.ArgumentParser(description="Publish the synthetic leaked-information validation case.")
    parser.add_argument("--odds-topic", default=os.getenv("KAFKA_TOPIC_ODDS", "odds-ticks"))
    parser.add_argument("--events-topic", default=os.getenv("KAFKA_TOPIC_EVENTS", "match-events"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--speed", type=float, default=0.0, help="Real seconds per simulated minute (0 = as fast as possible).")
    args = parser.parse_args()

    producer = build_producer(args.bootstrap_servers)
    print(f"[synthetic-leak-demo] publishing match_id='{MATCH_ID}' -- NOT real match data")
    publish_events(producer, args.events_topic)
    publish_odds(producer, args.odds_topic, args.speed)
    print("[synthetic-leak-demo] done.")


if __name__ == "__main__":
    main()
