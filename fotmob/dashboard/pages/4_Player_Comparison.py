"""
Page 4: Player Comparison

Compare 2–4 players side by side:
- KPI summary
- Scoring timeline (rolling avg overlay)
- Scoring bands (grouped bar)
- Points by result (grouped bar)
- Per-player match log tabs
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from db import (
    load_player_list,
    load_player_match_history,
    load_player_consistency,
)
from utils import setup_page, get_season

setup_page("Player Comparison · NWSL Fantasy")

season = get_season()

COLORS = ["#2E86C1", "#E74C3C", "#27AE60", "#F39C12"]

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.subheader("Select Players")

    all_players = load_player_list(season=season)
    player_options = sorted(all_players["player_name"].tolist())

    selected = []
    for i in range(4):
        suffix = " (required)" if i < 2 else " (optional)"
        p = st.selectbox(
            f"Player {i + 1}{suffix}",
            player_options,
            index=None,
            placeholder="Search by name...",
            key=f"cmp_p{i}",
        )
        if p:
            selected.append(p)

    st.subheader("Date Range")
    start_date = st.date_input("Start Date", value=None, key="cmp_start")
    end_date   = st.date_input("End Date",   value=None, key="cmp_end")

start_str = start_date.isoformat() if start_date else None
end_str   = end_date.isoformat()   if end_date   else None

# ── Page header ───────────────────────────────────────────────────────────────
st.title("Player Comparison")

# Deduplicate while preserving selection order
seen: set = set()
players = [p for p in selected if not (p in seen or seen.add(p))]

if len(players) < 2:
    st.info("Select at least 2 players in the sidebar to compare.")
    st.stop()

# ── Load data ─────────────────────────────────────────────────────────────────
histories: dict[str, pd.DataFrame] = {}
consistencies: dict = {}

for player in players:
    hist = load_player_match_history(player, season, start_str, end_str)
    if not hist.empty:
        hist["match_date"] = pd.to_datetime(hist["match_date"])
        histories[player] = hist

    c = load_player_consistency(player, season)
    consistencies[player] = c.iloc[0] if not c.empty else None

active = [p for p in players if p in histories]

if len(active) < 2:
    st.warning("Not enough match data found for the selected players.")
    st.stop()

# ── Row 1: KPI summary ────────────────────────────────────────────────────────
st.subheader("Season Summary")
cols = st.columns(len(active))

for i, player in enumerate(active):
    hist  = histories[player]
    c     = consistencies[player]
    color = COLORS[i]
    pos   = hist["draft_position"].iloc[0] if "draft_position" in hist.columns else "—"
    team  = hist["team_name"].iloc[-1]

    with cols[i]:
        st.markdown(
            f"<h4 style='color:{color}; margin-bottom:2px'>{player}</h4>",
            unsafe_allow_html=True,
        )
        st.caption(f"{pos} · {team}")

        total       = round(float(hist["total_points"].sum()), 1)
        gp          = len(hist)
        avg_pts     = round(float(c["avg_pts"]),       2) if c is not None and pd.notna(c["avg_pts"])       else None
        avg_last5   = round(float(c["avg_pts_last5"]), 2) if c is not None and pd.notna(c["avg_pts_last5"]) else None
        form_trend  = round(float(c["form_trend"]),    2) if c is not None and pd.notna(c["form_trend"])    else None
        consistency = round((hist["total_points"] > 0).mean() * 100, 1)

        st.metric("Season Total",  f"{total:.1f}  ({gp} games)")
        st.metric("Avg Pts/Match", f"{avg_pts:.2f}"  if avg_pts  is not None else "—")
        st.metric(
            "Avg Last 5",
            f"{avg_last5:.2f}" if avg_last5 is not None else "—",
            delta=f"{form_trend:+.2f}" if form_trend is not None else None,
        )
        st.metric("Consistency", f"{consistency:.0f}%")

st.divider()

# ── Row 2: Scoring timeline ───────────────────────────────────────────────────
st.subheader("Scoring Timeline")
st.caption("Lines = rolling 5-match average · dots = individual match points")

fig_tl = go.Figure()

for i, player in enumerate(active):
    hist  = histories[player]
    color = COLORS[i]

    fig_tl.add_trace(go.Scatter(
        x=hist["match_date"],
        y=hist["total_points"],
        mode="markers",
        name=player,
        marker=dict(color=color, size=6, opacity=0.35),
        showlegend=False,
        hovertemplate=f"{player}<br>%{{x|%b %d}}: %{{y}} pts<extra></extra>",
    ))

    if "rolling_5_avg" in hist.columns:
        fig_tl.add_trace(go.Scatter(
            x=hist["match_date"],
            y=hist["rolling_5_avg"],
            mode="lines",
            name=player,
            line=dict(color=color, width=2.5),
            hovertemplate=f"{player}<br>%{{x|%b %d}}: %{{y:.2f}} roll avg<extra></extra>",
        ))

fig_tl.update_layout(
    xaxis_title="Date",
    yaxis_title="Points",
    height=360,
    legend=dict(orientation="h", y=-0.22),
    margin=dict(t=10, b=10),
    hovermode="closest",
)
st.plotly_chart(fig_tl, use_container_width=True)

# ── Row 3: Scoring bands | Points by result ───────────────────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Scoring Bands")

    band_labels  = ["15+", "10–15", "5–10", "0–5", "Negative"]
    band_db_cols = ["matches_15_plus", "matches_10_to_15", "matches_5_to_10",
                    "matches_0_to_5", "matches_negative"]

    fig_bands = go.Figure()
    for i, player in enumerate(active):
        c = consistencies[player]
        if c is None:
            continue
        values = [int(c[col] or 0) for col in band_db_cols]
        fig_bands.add_trace(go.Bar(
            name=player,
            x=band_labels,
            y=values,
            marker_color=COLORS[i],
            hovertemplate=f"{player}<br>%{{x}}: %{{y}} matches<extra></extra>",
        ))

    fig_bands.update_layout(
        barmode="group",
        yaxis_title="Matches",
        height=310,
        margin=dict(t=10, b=10),
        legend=dict(orientation="h", y=-0.3),
    )
    st.plotly_chart(fig_bands, use_container_width=True)

with col_b:
    st.subheader("Points by Match Result")

    result_labels = ["Win", "Draw", "Loss"]
    result_cols   = ["avg_pts_win", "avg_pts_draw", "avg_pts_loss"]
    count_cols    = ["matches_won",  "matches_drawn",  "matches_lost"]

    fig_res = go.Figure()
    for i, player in enumerate(active):
        c = consistencies[player]
        if c is None:
            continue
        values = [float(c[col]) if pd.notna(c[col]) else 0 for col in result_cols]
        counts = [int(c[col] or 0) for col in count_cols]
        fig_res.add_trace(go.Bar(
            name=player,
            x=result_labels,
            y=values,
            marker_color=COLORS[i],
            text=[f"{v:.1f}" for v in values],
            textposition="outside",
            customdata=counts,
            hovertemplate=f"{player}<br>%{{x}}: %{{y:.2f}} avg pts (%{{customdata}} matches)<extra></extra>",
        ))

    fig_res.update_layout(
        barmode="group",
        yaxis_title="Avg Points",
        height=310,
        margin=dict(t=10, b=10),
        legend=dict(orientation="h", y=-0.3),
    )
    st.plotly_chart(fig_res, use_container_width=True)

# ── Row 4: Match log (tabbed) ─────────────────────────────────────────────────
with st.expander("Match Logs", expanded=False):
    tabs = st.tabs(active)
    for tab, player in zip(tabs, active):
        with tab:
            hist = histories[player]
            log  = hist[[
                "match_number", "match_date", "team_name", "opponent_name",
                "minutes_played", "total_points", "rolling_5_avg", "cumulative_avg",
            ]].copy()
            log["match_date"] = log["match_date"].dt.strftime("%Y-%m-%d")
            log.columns = ["#", "Date", "Team", "Opponent", "Min", "Pts", "Roll 5", "Avg"]
            st.dataframe(log, use_container_width=True, hide_index=True)
