"""Looks up an API-Football fixture id for a match by team + date.

You need this id for events_producer.py, and it's also the recommended
canonical --match-id to pass to odds_producer.py for the same match (team
names differ between football-data.co.uk and API-Football, so deriving a
shared id independently on each side doesn't work -- see odds_producer.py).

Usage:
    python -m producers.find_fixture --team Burnley --date 2023-08-11 --league 39 --season 2023
"""
from __future__ import annotations

import argparse
import os

import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = "https://v3.football.api-sports.io"


def find_fixtures(team: str, date: str, league: int, season: int, api_key: str) -> list[dict]:
    team_resp = requests.get(
        f"{API_BASE_URL}/teams",
        headers={"x-apisports-key": api_key},
        params={"search": team},
        timeout=30,
    )
    team_resp.raise_for_status()
    team_matches = team_resp.json()["response"]
    if not team_matches:
        raise ValueError(f"No team found matching '{team}'")
    team_id = team_matches[0]["team"]["id"]
    team_name = team_matches[0]["team"]["name"]

    fixtures_resp = requests.get(
        f"{API_BASE_URL}/fixtures",
        headers={"x-apisports-key": api_key},
        params={"league": league, "season": season, "team": team_id, "from": date, "to": date},
        timeout=30,
    )
    fixtures_resp.raise_for_status()
    payload = fixtures_resp.json()
    if payload.get("errors"):
        raise RuntimeError(f"API-Football error: {payload['errors']}")

    print(f"Resolved '{team}' -> {team_name} (id {team_id})")
    return payload["response"]


def main():
    parser = argparse.ArgumentParser(description="Find an API-Football fixture id by team + date.")
    parser.add_argument("--team", required=True)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--league", type=int, default=39, help="API-Football league id (default 39 = Premier League).")
    parser.add_argument("--season", type=int, default=2023, help="Season start year (default 2023 = 2023/24).")
    args = parser.parse_args()

    api_key = os.getenv("API_FOOTBALL_KEY")
    if not api_key:
        raise SystemExit("API_FOOTBALL_KEY not set -- copy .env.example to .env and fill it in.")

    fixtures = find_fixtures(args.team, args.date, args.league, args.season, api_key)
    if not fixtures:
        print("No fixtures found for that team/date/league/season.")
        return

    for f in fixtures:
        home = f["teams"]["home"]["name"]
        away = f["teams"]["away"]["name"]
        score = f"{f['goals']['home']}-{f['goals']['away']}"
        print(f"fixture_id={f['fixture']['id']}  {home} vs {away}  {score}  ({f['fixture']['date']})")


if __name__ == "__main__":
    main()
