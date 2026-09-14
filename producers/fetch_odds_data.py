"""Pulls a season's real odds CSV from football-data.co.uk (no signup needed).

Usage:
    python -m producers.fetch_odds_data --division E0 --season 2324
"""
from __future__ import annotations

import argparse
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
BASE_URL = "https://www.football-data.co.uk/mmz4281"


def fetch(division: str, season: str, out_dir: Path) -> Path:
    url = f"{BASE_URL}/{season}/{division}.csv"
    out_path = out_dir / f"{division}_{season}.csv"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(response.content)
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Fetch a season's odds CSV from football-data.co.uk.")
    parser.add_argument("--division", default="E0", help="League code, e.g. E0 = Premier League.")
    parser.add_argument("--season", default="2324", help="Season code, e.g. 2324 = 2023/24.")
    parser.add_argument("--out-dir", default=str(REPO_ROOT / "data/raw"))
    args = parser.parse_args()

    path = fetch(args.division, args.season, Path(args.out_dir))
    print(f"[fetch-odds-data] saved {path}")


if __name__ == "__main__":
    main()
