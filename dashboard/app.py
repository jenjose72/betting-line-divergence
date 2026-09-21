"""Line Watch dashboard: live-ish view of a replayed match's odds vs. events,
with the Spark job's divergence score overlaid.

Reads straight off HDFS (see hdfs_reader.py for why not Hive/WebHDFS).
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from hdfs_reader import read_table

st.set_page_config(page_title="Line Watch", layout="wide")

EVENT_COLORS = {
    "Goal": "#2ecc71",
    "Red Card": "#e74c3c",
    "Yellow Card": "#f1c40f",
    "Penalty Missed": "#e67e22",
    "Substitution": "#95a5a6",
}


@st.cache_data(ttl=10)
def load_tables():
    return (
        read_table("odds_ticks_raw"),
        read_table("match_events_raw"),
        read_table("divergence_scores"),
    )


def render_match(odds: pd.DataFrame, events: pd.DataFrame, scores: pd.DataFrame, match_id: str):
    odds = odds[odds["match_id"] == match_id].sort_values("minute")
    events = events[events["match_id"] == match_id].sort_values("minute")
    scores = scores[scores["match_id"] == match_id].sort_values("window_start")

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.7, 0.3],
        vertical_spacing=0.06,
        subplot_titles=("Odds (decimal)", "Divergence score per 5-min window"),
    )

    for col, name, color in [
        ("home_odds", "Home", "#3498db"),
        ("draw_odds", "Draw", "#95a5a6"),
        ("away_odds", "Away", "#e74c3c"),
    ]:
        fig.add_trace(
            go.Scatter(x=odds["minute"], y=odds[col], name=name, mode="lines", line=dict(color=color, width=2)),
            row=1,
            col=1,
        )

    y_max = odds[["home_odds", "draw_odds", "away_odds"]].to_numpy().max() if not odds.empty else 1
    seen_types = set()
    for _, ev in events.iterrows():
        color = EVENT_COLORS.get(ev["type"], "#7f8c8d")
        show_legend = ev["type"] not in seen_types
        seen_types.add(ev["type"])
        fig.add_trace(
            go.Scatter(
                x=[ev["minute"], ev["minute"]],
                y=[0, y_max * 1.05],
                mode="lines",
                line=dict(color=color, width=1, dash="dot"),
                name=ev["type"],
                legendgroup=ev["type"],
                showlegend=show_legend,
                hovertext=f"{ev['type']} ({ev['team']}) - {ev['player'] or ''} @ min {ev['minute']}",
                hoverinfo="text",
            ),
            row=1,
            col=1,
        )

    bar_colors = ["#e74c3c" if f else "#3498db" for f in scores["flagged"]]
    fig.add_trace(
        go.Bar(
            x=scores["window_start"],
            y=scores["divergence_score"],
            marker_color=bar_colors,
            name="divergence_score",
            hovertext=[
                f"window {w}-{w+5}: actual={a:.4f} expected={e:.4f} events={t or 'none'}"
                for w, a, e, t in zip(
                    scores["window_start"], scores["actual_move"], scores["expected_move"], scores["event_types"]
                )
            ],
            hoverinfo="text",
            showlegend=False,
        ),
        row=2,
        col=1,
    )
    fig.add_hline(y=0, line_width=1, line_color="black", row=2, col=1)

    fig.update_xaxes(title_text="Match minute", row=2, col=1)
    fig.update_yaxes(title_text="Decimal odds", row=1, col=1)
    fig.update_yaxes(title_text="Divergence", row=2, col=1)
    fig.update_layout(height=650, hovermode="closest", legend=dict(orientation="h", y=1.12))

    st.plotly_chart(fig, use_container_width=True)

    flagged_count = int(scores["flagged"].sum())
    if flagged_count:
        st.warning(f"{flagged_count} window(s) flagged — odds moved more than the events in that window explain.")
    else:
        st.info("No windows flagged for this match — actual odds movement stayed within what the events explain.")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Divergence by window")
        st.dataframe(
            scores[["window_start", "n_events", "event_types", "actual_move", "expected_move", "divergence_score", "flagged"]],
            hide_index=True,
            use_container_width=True,
        )
    with col2:
        st.subheader("Event timeline")
        st.dataframe(events[["minute", "type", "team", "player"]], hide_index=True, use_container_width=True)


def main():
    st.title("Line Watch")
    st.caption("Real-time betting-line divergence detection — Kafka -> Spark Structured Streaming -> HDFS/Hive")

    if st.button("Refresh"):
        st.cache_data.clear()

    odds, events, scores = load_tables()

    if odds.empty:
        st.warning(
            "No data yet. Make sure the Docker stack, the producers, and streaming/divergence_job.py have all run — "
            "see the top-level README for setup steps."
        )
        return

    match_ids = sorted(odds["match_id"].unique())
    match_id = st.sidebar.selectbox("Match", match_ids)
    st.sidebar.caption(f"{len(match_ids)} match(es) currently replayed")

    render_match(odds, events, scores, match_id)


if __name__ == "__main__":
    main()
