"""Shared helpers: page config + global sidebar (called from each page)."""
from datetime import date
import streamlit as st

_DEFAULT_CUTOFF = date(2026, 3, 13)


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


def get_cutoff_date() -> date | None:
    """Return the global data cutoff date from session state."""
    return st.session_state.get("cutoff_date", _DEFAULT_CUTOFF)


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
        st.date_input("Data cutoff", value=_DEFAULT_CUTOFF, key="cutoff_date")

        st.divider()
        if st.button("Refresh All Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
