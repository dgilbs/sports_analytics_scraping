"""
Page 1: Player Performance

Charts:
1. Match timeline (bars colored by score tier + rolling avg lines)
2. Season points breakdown (horizontal stacked bar)
3. Scoring band distribution
4. Context splits (home/away/vs-strong/vs-weak)
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
    load_player_season_totals,
    load_player_consistency,
    load_player_position_stats,
)
from utils import setup_page, get_season

setup_page("Player Performance · NWSL Fantasy")

# ── Sidebar: season → team → player ──────────────────────────────────────────
season = get_season()

from db import load_teams

with st.sidebar:
    st.subheader("Find a Player")

    all_teams = load_teams(season)
    selected_team = st.selectbox(
        "Filter by team (optional)", all_teams, index=None, placeholder="All teams..."
    )

    all_players = load_player_list(season=season, team=selected_team)
    player_options = sorted(all_players["player_name"].tolist())

    player_name = st.selectbox(
        "Player", player_options,
        index=None,
        placeholder="Search by name...",
    )

    st.subheader("Date Range")
    start_date = st.date_input("Start Date", value=None)
    end_date   = st.date_input("End Date",   value=None)

start_str = start_date.isoformat() if start_date is not None else None
end_str   = end_date.isoformat()   if end_date   is not None else None

# ── Empty state ───────────────────────────────────────────────────────────────
if not player_name:
    st.title("Player Performance")
    st.info("Use the sidebar to select a team and player.")
    st.stop()

# ── Load data ─────────────────────────────────────────────────────────────────
match_hist = load_player_match_history(player_name, season, start_str, end_str)
season_totals = load_player_season_totals(player_name, season)
consistency = load_player_consistency(player_name, season)
pos_stats = load_player_position_stats(player_name, season)

if match_hist.empty:
    st.title(f"{player_name}")
    st.info("No match history found for this player.")
    st.stop()

match_hist["match_date"] = pd.to_datetime(match_hist["match_date"])

# Pull scalar values (first row if multiple)
c = consistency.iloc[0] if not consistency.empty else None
s = season_totals.iloc[0] if not season_totals.empty else None

# ── Row 1: KPI Metrics ────────────────────────────────────────────────────────
st.title(f"{player_name}")
_pos = match_hist["draft_position"].iloc[0] if "draft_position" in match_hist.columns else ""
_team = match_hist["team_name"].iloc[-1] if not match_hist.empty else ""
st.caption(f"{_pos} · {_team}")

# ── Most recent match ─────────────────────────────────────────────────────────
last = match_hist.iloc[-1]
_last_pts = last["total_points"]
_last_date = pd.Timestamp(last["match_date"]).strftime("%b %d, %Y")
_last_opp  = last["opponent_name"]
_pts_color = "green" if _last_pts >= 10 else ("orange" if _last_pts >= 5 else ("red" if _last_pts < 0 else "gray"))
st.markdown(
    f"**Last match:** {_last_date} vs {_last_opp} — "
    f"<span style='color:{_pts_color}; font-weight:bold'>{_last_pts} pts</span>",
    unsafe_allow_html=True,
)

m1, m2, m3, m4 = st.columns(4)

season_total = float(s["total_points"]) if s is not None else None
avg_pts = float(c["avg_pts"]) if c is not None else None
avg_last5 = float(c["avg_pts_last5"]) if c is not None and c["avg_pts_last5"] is not None else None
form_trend = float(c["form_trend"]) if c is not None and c["form_trend"] is not None else None

with m1:
    st.metric("Season Total", f"{season_total:.1f}" if season_total is not None else "—")
with m2:
    st.metric("Avg Pts/Match", f"{avg_pts:.2f}" if avg_pts is not None else "—")
with m3:
    delta_str = f"{form_trend:+.2f}" if form_trend is not None else None
    st.metric("Avg Last 5", f"{avg_last5:.2f}" if avg_last5 is not None else "—", delta=delta_str)
with m4:
    trend_label = "Improving" if (form_trend or 0) > 0 else ("Declining" if (form_trend or 0) < 0 else "Flat")
    st.metric("Form Trend", trend_label, delta=delta_str)

st.divider()

# ── Row 2: Timeline (60%) | Points Breakdown (40%) ───────────────────────────
col_left, col_right = st.columns([6, 4])

# Chart 1: Match Timeline
with col_left:
    st.subheader("Match Timeline")

    def _bar_color(pts):
        if pts >= 10:
            return "#27AE60"
        elif pts >= 5:
            return "#17BECF"
        elif pts >= 0:
            return "#AAAAAA"
        else:
            return "#E74C3C"

    bar_colors = [_bar_color(p) for p in match_hist["total_points"]]
    hover_text = [
        f"GW{row.match_number} vs {row.opponent_name}<br>"
        f"Date: {pd.Timestamp(row.match_date).strftime('%b %d')}<br>"
        f"Points: {row.total_points}"
        for row in match_hist.itertuples()
    ]

    toggle_col1, toggle_col2 = st.columns(2)
    show_rolling = toggle_col1.checkbox("Show Rolling 5 Avg", value=True)
    show_season  = toggle_col2.checkbox("Show Season Avg",    value=True)

    fig_tl = go.Figure()
    fig_tl.add_trace(go.Bar(
        x=match_hist["match_date"],
        y=match_hist["total_points"],
        marker_color=bar_colors,
        name="Points",
        hovertext=hover_text,
        hoverinfo="text",
    ))
    if show_rolling:
        fig_tl.add_trace(go.Scatter(
            x=match_hist["match_date"],
            y=match_hist["rolling_5_avg"],
            mode="lines",
            name="Rolling 5 Avg",
            line=dict(color="orange", width=2),
        ))
    if show_season:
        fig_tl.add_trace(go.Scatter(
            x=match_hist["match_date"],
            y=match_hist["cumulative_avg"],
            mode="lines",
            name="Season Avg",
            line=dict(color="gray", width=1.5, dash="dash"),
        ))
    fig_tl.update_layout(
        xaxis_title="Date",
        yaxis_title="Points",
        height=320,
        legend=dict(orientation="h", y=-0.25),
        margin=dict(t=10, b=10),
    )
    st.plotly_chart(fig_tl, use_container_width=True)
    st.caption(
        "Bar color indicates points scored: "
        "🟢 **10+ pts** · 🔵 **5–9 pts** · ⬜ **0–4 pts** · 🔴 **negative**"
    )

# Chart 2: Season Points Breakdown
with col_right:
    st.subheader("Season Breakdown")

    if s is None:
        st.info("Season totals not available.")
    else:
        # Divisors to convert pts → raw count, keyed by pts column
        goal_div  = {"GK": 10, "DF": 6, "MF": 5, "FW": 4}.get(_pos, 4)
        cs_div    = {"GK": 5,  "DF": 4, "MF": 2, "FW": 1}.get(_pos, 1)
        tckl_div  = {"FW": 1,  "MF": 0.5, "DF": 0.5, "GK": 0}.get(_pos, 0.5)
        gc_div    = {"GK": 1,  "DF": 0.5}.get(_pos, 1)

        # (pts_col, bar_color, divisor, display_label)
        pos_components = [
            ("pts_appearance",        "#3498DB", 1,        "Appearances"),
            ("pts_60_minutes",        "#2980B9", 2,        "60+ Min Games"),
            ("pts_goals",             "#27AE60", goal_div, "Goals"),
            ("pts_assists",           "#1ABC9C", 2,        "Assists"),
            ("pts_clean_sheet",       "#16A085", cs_div,   "Clean Sheets"),
            ("pts_saves",             "#8E44AD", 0.5,      "Saves"),
            ("pts_tackles",           "#2ECC71", tckl_div, "Tackles"),
            ("pts_interceptions",     "#1E8449", 0.5,      "Interceptions"),
            ("pts_blocks",            "#117A65", 0.5,      "Blocks"),
            ("pts_successful_takeons","#F39C12", 0.5,      "Take-ons"),
            ("pts_touches",           "#E67E22", 2,        "Touches Bonus"),
            ("pts_pass_completion",   "#D35400", 2,        "Pass Bonus"),
            ("pts_penalty_save",      "#6C3483", 5,        "Pen Saves"),
        ]
        neg_components = [
            ("pts_yellow_cards",   "#F1C40F", -2,      "Yellow Cards"),
            ("pts_red_card",       "#E74C3C", -6,      "Red Cards"),
            ("pts_goals_conceded", "#C0392B", -gc_div, "Goals Conceded"),
            ("pts_penalty_missed", "#922B21", -3,      "Pens Missed"),
            ("pts_own_goal",       "#7B241C", -3,      "Own Goals"),
        ]

        fig_bd = go.Figure()

        for col, color, div, label in pos_components:
            pts = float(s[col]) if col in s.index and pd.notna(s[col]) else 0
            if pts == 0 or div == 0:
                continue
            raw = round(pts / div)
            fig_bd.add_trace(go.Bar(
                name=label, x=[raw], y=["Season"],
                orientation="h", marker_color=color,
                hovertemplate=f"{label}: %{{x}}<extra></extra>",
            ))

        for col, color, div, label in neg_components:
            pts = float(s[col]) if col in s.index and pd.notna(s[col]) else 0
            if pts == 0 or div == 0:
                continue
            raw = round(pts / div)
            fig_bd.add_trace(go.Bar(
                name=label, x=[-raw], y=["Season"],
                orientation="h", marker_color=color,
                hovertemplate=f"{label}: %{{x}}<extra></extra>",
            ))

        fig_bd.update_layout(
            barmode="relative",
            xaxis_title="Count",
            height=200,
            showlegend=True,
            legend=dict(font=dict(size=9), orientation="v"),
            margin=dict(t=10, b=10, r=120),
        )
        st.plotly_chart(fig_bd, use_container_width=True)

# ── Row 3: Scoring Bands (50%) | Points by Result (50%) ─────────────────────
col_a, col_b = st.columns(2)

# Chart 3: Scoring Bands
with col_a:
    st.subheader("Scoring Bands")

    if c is None:
        st.info("Consistency data not available.")
    else:
        bands = {
            "15+": int(c["matches_15_plus"] or 0),
            "10–15": int(c["matches_10_to_15"] or 0),
            "5–10": int(c["matches_5_to_10"] or 0),
            "0–5": int(c["matches_0_to_5"] or 0),
            "Negative": int(c["matches_negative"] or 0),
        }
        band_colors = ["#1A5276", "#2E86C1", "#85C1E9", "#AEB6BF", "#E74C3C"]

        fig_bands = go.Figure(go.Bar(
            x=list(bands.keys()),
            y=list(bands.values()),
            marker_color=band_colors,
            text=list(bands.values()),
            textposition="outside",
            hovertemplate="%{x}: %{y} matches<extra></extra>",
        ))
        fig_bands.update_layout(
            yaxis_title="Matches",
            height=280,
            margin=dict(t=10, b=10),
        )
        st.plotly_chart(fig_bands, use_container_width=True)

# Chart 4: Points by Match Result
with col_b:
    st.subheader("Points by Match Result")

    if c is None:
        st.info("Context data not available.")
    else:
        result_labels = ["Win", "Draw", "Loss"]
        result_cols = ["avg_pts_win", "avg_pts_draw", "avg_pts_loss"]
        result_colors = ["#27AE60", "#F39C12", "#E74C3C"]
        result_counts = [int(c[col] or 0) for col in ["matches_won", "matches_drawn", "matches_lost"]]
        result_vals = [float(c[col]) if pd.notna(c[col]) else 0 for col in result_cols]

        fig_splits = go.Figure()
        fig_splits.add_trace(go.Bar(
            x=result_labels,
            y=result_vals,
            marker_color=result_colors,
            text=[f"{v:.1f}" for v in result_vals],
            textposition="outside",
            customdata=result_counts,
            hovertemplate="%{x}: %{y:.2f} avg pts<br>(%{customdata} matches)<extra></extra>",
            name="Avg Pts",
        ))
        if avg_pts is not None:
            fig_splits.add_hline(
                y=avg_pts,
                line_dash="dash",
                line_color="gray",
                annotation_text=f"Season avg: {avg_pts:.2f}",
                annotation_position="right",
            )
        fig_splits.update_layout(
            yaxis_title="Avg Points",
            height=280,
            margin=dict(t=10, b=30),
            showlegend=False,
        )
        st.plotly_chart(fig_splits, use_container_width=True)
        st.caption(
            "Average fantasy points by team match result. "
            "A large gap between Win and Loss suggests the player's output is closely tied to team performance."
        )

# ── Row 4: Performance by Opponent ───────────────────────────────────────────
with st.expander("Performance by Opponent", expanded=False):
    opp_df = (
        match_hist.groupby("opponent_name")
        .agg(
            matches=("total_points", "count"),
            total_pts=("total_points", "sum"),
            avg_pts=("total_points", "mean"),
        )
        .round({"total_pts": 1, "avg_pts": 2})
        .sort_values("avg_pts", ascending=False)
        .reset_index()
    )
    opp_df.rename(columns={"opponent_name": "Opponent", "matches": "GP",
                            "total_pts": "Total Pts", "avg_pts": "Avg Pts"}, inplace=True)

    fig_opp = go.Figure(go.Bar(
        x=opp_df["Avg Pts"],
        y=opp_df["Opponent"],
        orientation="h",
        marker_color="#2E86C1",
        text=opp_df["Avg Pts"].apply(lambda v: f"{v:.2f}"),
        textposition="outside",
        customdata=opp_df[["GP", "Total Pts"]].values,
        hovertemplate="%{y}<br>Avg: %{x:.2f} pts<br>%{customdata[0]} games · %{customdata[1]:.1f} total<extra></extra>",
    ))
    if avg_pts is not None:
        fig_opp.add_vline(
            x=avg_pts,
            line_dash="dash",
            line_color="gray",
            annotation_text=f"Season avg: {avg_pts:.2f}",
            annotation_position="top right",
        )
    fig_opp.update_layout(
        xaxis_title="Avg Points",
        height=max(300, len(opp_df) * 28 + 60),
        margin=dict(t=20, b=40, l=140),
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(fig_opp, use_container_width=True)
    st.dataframe(opp_df, use_container_width=True, hide_index=True)

# ── Row 5: Full Match Log ─────────────────────────────────────────────────────
with st.expander("Full Match Log", expanded=False):
    log_df = match_hist[[
        "match_number", "match_date", "team_name", "opponent_name", "minutes_played",
        "total_points", "rolling_5_avg", "cumulative_avg",
        "pts_goals", "pts_assists", "pts_clean_sheet", "pts_saves",
        "pts_yellow_cards", "pts_red_card",
    ]].copy()

    # Convert pts columns → raw counts using position-specific divisors
    log_df["pts_goals"]        = (log_df["pts_goals"] / goal_div).round().astype("Int64")
    log_df["pts_assists"]      = (log_df["pts_assists"] / 2).round().astype("Int64")
    log_df["pts_clean_sheet"]  = log_df["pts_clean_sheet"].apply(
        lambda v: 1 if v and v > 0 else 0).astype("Int64")
    log_df["pts_saves"]        = (log_df["pts_saves"] / 0.5).round().astype("Int64")
    log_df["pts_yellow_cards"] = (log_df["pts_yellow_cards"] / -2).round().astype("Int64")
    log_df["pts_red_card"]     = (log_df["pts_red_card"] / -6).round().astype("Int64")

    log_df["match_date"] = log_df["match_date"].dt.strftime("%Y-%m-%d")
    log_df.columns = [
        "#", "Date", "Team", "Opponent", "Min",
        "Pts", "Roll 5", "Avg",
        "Goals", "Assists", "CS", "Saves", "Yellows", "Reds",
    ]
    st.dataframe(log_df, use_container_width=True)
