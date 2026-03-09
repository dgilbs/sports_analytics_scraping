"""
Page 2: Player Leaderboard

Table of all players ranked by season total fantasy points,
with filters for position, team, and home/away.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import date

import pandas as pd
import streamlit as st

from db import load_leaderboard, load_teams
from utils import setup_page, get_season

setup_page("Leaderboard · NWSL Fantasy")

season = get_season()

# ── Sidebar filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.subheader("Filters")

    pos_filter = st.multiselect(
        "Fantasy Position",
        ["GK", "DF", "MF", "FW"],
        default=[],
        placeholder="All positions",
    )

    all_teams = load_teams(season)
    team_filter = st.multiselect(
        "NWSL Team",
        all_teams,
        default=[],
        placeholder="All teams",
    )

    side_filter = st.radio(
        "Home / Away",
        ["All", "Home", "Away"],
        horizontal=True,
    )

    st.subheader("Date Range")
    start_date = st.date_input("Start Date", value=None)
    end_date   = st.date_input("End Date",   value=date(2025, 11, 3))

# Convert to ISO strings so cache hashing is reliable
start_str = start_date.isoformat() if start_date is not None else None
end_str   = end_date.isoformat()   if end_date   is not None else None

# ── Load data ─────────────────────────────────────────────────────────────────
df = load_leaderboard(
    season=season,
    positions=tuple(pos_filter) if pos_filter else None,
    teams=tuple(team_filter) if team_filter else None,
    side=side_filter.lower() if side_filter != "All" else None,
    start_date=start_str,
    end_date=end_str,
)

st.title("Player Leaderboard")

if df.empty:
    st.info("No players match the current filters.")
    st.stop()

# ── Rank column ───────────────────────────────────────────────────────────────
df.insert(0, "rank", range(1, len(df) + 1))

# ── Styling ───────────────────────────────────────────────────────────────────
def _color_trend(val):
    if pd.isna(val):
        return ""
    if val > 1:
        return "background-color: #1E8449; color: white"
    elif val > 0:
        return "background-color: #82E0AA"
    elif val < -1:
        return "background-color: #922B21; color: white"
    elif val < 0:
        return "background-color: #F1948A"
    return ""

styled = (
    df.style
    .applymap(_color_trend, subset=["form_trend"])
    .format({
        "total_pts":       "{:.1f}",
        "avg_pts":         "{:.2f}",
        "avg_pts_last5":   "{:.2f}",
        "form_trend":      "{:+.2f}",
        "season_high":     "{:.1f}",
        "season_low":      "{:.1f}",
        "consistency_pct": "{:.0f}%",
    }, na_rep="—")
)

st.dataframe(styled, use_container_width=True, height=700)

with st.expander("Metric Definitions"):
    st.markdown("""
| Metric | Description |
|--------|-------------|
| **Rank** | Overall rank by total points within the current filters |
| **Position** | Fantasy position (GK / DF / MF / FW) |
| **Player** | Player name |
| **Team** | Current team; shows both teams separated by " / " if the player transferred mid-season |
| **Matches Played** | Number of matches included under the current filters |
| **Total Pts** | Sum of all fantasy points earned across those matches |
| **Avg Pts** | Total points divided by matches played |
| **Avg Pts Last 5** | Average points across the 5 most recent matches (within the filtered date range) |
| **Form Trend** | Avg pts last 5 minus season avg — positive means improving, negative means declining |
| **Season High** | Highest single-match points total |
| **Season Low** | Lowest single-match points total |
| **Consistency %** | Percentage of matches where the player scored more than 0 fantasy points |
    """)
