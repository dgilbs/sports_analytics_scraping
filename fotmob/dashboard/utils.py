"""Shared helpers: page config + global sidebar (called from each page)."""
from datetime import date
import streamlit as st

# ── Backend cutoff configuration ─────────────────────────────────────────────
# Update these dates to control how far forward data is shown in the dashboard.
# No UI filter is exposed to users — change these values to update the cutoff.
SEASON_CUTOFFS: dict[str, date] = {
    "2026": date(2026, 3, 23),
}
_DEFAULT_CUTOFF: date = date(2026, 3, 23)
# ─────────────────────────────────────────────────────────────────────────────


def setup_page(title: str = "NWSL Fantasy Dashboard") -> None:
    st.set_page_config(
        page_title=title,
        page_icon="⚽",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _global_sidebar()


def get_season() -> str | None:
    """Return the currently selected season from session state."""
    return st.session_state.get("selected_season")


def get_cutoff_date(season: str | None = None) -> date:
    """Return the backend cutoff date for the given season."""
    if season and season in SEASON_CUTOFFS:
        return SEASON_CUTOFFS[season]
    return _DEFAULT_CUTOFF


def _global_sidebar() -> None:
    from db import load_seasons  # local import avoids circular at module level

    with st.sidebar:
        st.title("NWSL Fantasy")
        st.divider()

        # Season selector
        seasons_df = load_seasons()
        if not seasons_df.empty:
            options = seasons_df["season"].tolist()  # already DESC
            st.selectbox("Season", options, key="selected_season")
        else:
            st.session_state["selected_season"] = None
            st.caption("No season data yet.")

        st.divider()
        if st.button("Refresh All Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
