"""Models a match's intra-game odds movement from just its opening and closing lines.

football-data.co.uk only gives pre-match (opening) and closing odds, not true
intra-match ticks (see PROJECT_PLAN.md's stated modeling assumption). This
module fills that gap: movement between open and close is modeled as a mix of
slow background drift (the market isn't perfectly still even absent news) plus
sharp event-driven jumps weighted toward the minute each event happens, so a
goal moves the line hard right around when it happens and a yellow card moves
it only slightly.

Interpolation happens in de-vigged probability space, not raw decimal odds,
since probability is what events actually change -- decimal odds are just its
inverse, and interpolating them directly would distort the shape of the move.
"""
from __future__ import annotations

import math

EVENT_WEIGHTS = {
    "Goal": 1.0,
    "Red Card": 0.5,
    "Penalty Missed": 0.3,
    "Yellow Card": 0.15,
    "Substitution": 0.05,
}
DEFAULT_EVENT_WEIGHT = 0.1

# Fraction of total movement attributed to slow background drift vs.
# event-driven jumps. Stated design choice -- see PROJECT_PLAN.md Section 2.
BACKGROUND_DRIFT_ALPHA = 0.3

# How sharply the line reacts around an event's minute (higher = snappier).
SIGMOID_STEEPNESS = 1.5


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def _implied_probs(h: float, d: float, a: float) -> tuple[float, float, float, float]:
    """Returns (p_home, p_draw, p_away, overround) from decimal odds."""
    raw = [1 / h, 1 / d, 1 / a]
    overround = sum(raw)
    return raw[0] / overround, raw[1] / overround, raw[2] / overround, overround


def _weight_curve(minutes: list[int], events: list[dict], duration: int) -> list[float]:
    """Monotonic 0->1 curve: how much of the open->close move has 'happened' by each minute."""
    background = [m / duration for m in minutes]

    if not events:
        return background

    raw_weights = [EVENT_WEIGHTS.get(e["type"], DEFAULT_EVENT_WEIGHT) for e in events]
    total = sum(raw_weights)

    def event_curve(m: float) -> float:
        return sum(
            w * _sigmoid(SIGMOID_STEEPNESS * (m - e["minute"]))
            for w, e in zip(raw_weights, events)
        ) / total

    # Normalize so the event-driven component reaches exactly 1.0 at full time.
    end_value = event_curve(duration)
    scale = 1.0 / end_value if end_value > 0 else 1.0

    return [
        BACKGROUND_DRIFT_ALPHA * bg + (1 - BACKGROUND_DRIFT_ALPHA) * (event_curve(m) * scale)
        for bg, m in zip(background, minutes)
    ]


def interpolate_odds(
    open_odds: tuple[float, float, float],
    close_odds: tuple[float, float, float],
    events: list[dict],
    duration: int = 90,
) -> list[dict]:
    """Returns one tick per minute (0..duration inclusive).

    `events` is a list of {"minute": int, "type": str} dicts (type keys into
    EVENT_WEIGHTS above; unknown types fall back to DEFAULT_EVENT_WEIGHT).
    """
    ph_o, pd_o, pa_o, or_o = _implied_probs(*open_odds)
    ph_c, pd_c, pa_c, or_c = _implied_probs(*close_odds)

    minutes = list(range(0, duration + 1))
    weights = _weight_curve(minutes, events, duration)

    ticks = []
    for m, w in zip(minutes, weights):
        ph = (1 - w) * ph_o + w * ph_c
        pd = (1 - w) * pd_o + w * pd_c
        pa = (1 - w) * pa_o + w * pa_c
        overround = (1 - w) * or_o + w * or_c

        ticks.append(
            {
                "minute": m,
                "home_odds": round(1 / (overround * ph), 3),
                "draw_odds": round(1 / (overround * pd), 3),
                "away_odds": round(1 / (overround * pa), 3),
            }
        )
    return ticks
