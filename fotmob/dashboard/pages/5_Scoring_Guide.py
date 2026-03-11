"""
Page 3: Scoring Guide

Static reference page explaining the fantasy points system.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import streamlit as st
from utils import setup_page

setup_page("Scoring Guide · NWSL Fantasy")

st.title("Scoring Guide")
st.caption("How fantasy points are calculated for each position.")

# ── Universal ─────────────────────────────────────────────────────────────────
st.subheader("All Positions")
st.table({
    "Action": [
        "Appearance (any minutes)",
        "60+ minutes played",
        "Assist",
        "Interception",
        "Block",
        "Successful take-on",
        "60+ touches",
        "85%+ pass accuracy (20+ attempts)",
        "Yellow card",
        "Red card",
        "Penalty won",
        "Penalty missed",
        "Own goal",
    ],
    "Points": [
        "+1", "+2", "+2", "+0.5", "+0.5", "+0.5",
        "+2", "+2",
        "−2 (max −4)", "−6", "+2", "−3", "−3",
    ],
})

# ── Position-specific ─────────────────────────────────────────────────────────
st.subheader("Position-Specific")

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Goalkeeper (GK)**")
    st.table({
        "Action": ["Goal", "Clean sheet", "Save", "Penalty save", "Goal conceded"],
        "Points": ["+10", "+5", "+0.5", "+5", "−1 each"],
    })

    st.markdown("**Defender (DF)**")
    st.table({
        "Action": ["Goal", "Clean sheet (60+ min)", "Tackle won", "Goal conceded"],
        "Points": ["+6", "+4", "+0.5", "−0.5 each"],
    })

with col2:
    st.markdown("**Midfielder (MF)**")
    st.table({
        "Action": ["Goal", "Clean sheet", "Tackle won"],
        "Points": ["+5", "+2", "+0.5"],
    })

    st.markdown("**Forward (FW)**")
    st.table({
        "Action": ["Goal", "Tackle won"],
        "Points": ["+4", "+1"],
    })

st.divider()
st.caption(
    "Stats sourced from FotMob match reports. "
    "Cards, own goals, and penalty data are cross-referenced from FBRef where available — "
    "players without a FBRef crossref entry will show 0 for those categories."
)
