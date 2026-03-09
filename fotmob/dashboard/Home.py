"""
NWSL Fantasy In-Season Dashboard — entry point.

Run:  streamlit run fotmob/dashboard/app.py
"""
import streamlit as st
from utils import setup_page

setup_page("NWSL Fantasy Dashboard")

st.title("NWSL Fantasy In-Season Dashboard")
st.markdown("""
Use **Player Deep Dive** in the sidebar to explore match timelines, scoring bands,
and context splits for any player.

---
*Data sourced from FotMob via dbt views in Neon Postgres. Cache refreshes every 5 minutes,
or hit **Refresh All Data** in the sidebar.*
""")
