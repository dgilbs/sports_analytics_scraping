"""
DB connection + cached query functions for the NWSL Fantasy dashboard.

All views live in the fotmob schema on Neon Postgres.
Env vars required: NEON_HOST, NEON_USER, NEON_PASSWORD, NEON_DBNAME
Optional:         NEON_PORT (default 5432)
"""
import os

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(usecwd=True))

# On Streamlit Cloud, secrets aren't in os.environ — inject them so the rest
# of the code works the same locally and in production.
try:
    for _k, _v in st.secrets.items():
        if _k not in os.environ:
            os.environ[_k] = str(_v)
except Exception:
    pass

_SCHEMA = "fotmob"


# ── Connection ────────────────────────────────────────────────────────────────

@st.cache_resource(validate=lambda conn: conn.closed == 0)
def get_conn():
    return psycopg2.connect(
        host=os.environ["NEON_HOST"],
        user=os.environ["NEON_USER"],
        password=os.environ["NEON_PASSWORD"],
        dbname=os.environ["NEON_DBNAME"],
        port=int(os.environ.get("NEON_PORT", 5432)),
        sslmode="require",
    )


def _q(sql: str, params=None) -> pd.DataFrame:
    """Execute SQL and return a DataFrame, reconnecting if the connection dropped."""
    try:
        return pd.read_sql(sql, get_conn(), params=params)
    except Exception:
        get_conn.clear()
        return pd.read_sql(sql, get_conn(), params=params)


# ── Leaderboard ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_opponents(season: str | None = None) -> list[str]:
    """Sorted list of opponent names for the filter widget."""
    if season:
        df = _q(f"""
            SELECT DISTINCT opponent_name
            FROM {_SCHEMA}.fantasy_match_points
            WHERE draft_position IS NOT NULL AND opponent_name IS NOT NULL AND season = %s
            ORDER BY opponent_name
        """, (season,))
    else:
        df = _q(f"""
            SELECT DISTINCT opponent_name
            FROM {_SCHEMA}.fantasy_match_points
            WHERE draft_position IS NOT NULL AND opponent_name IS NOT NULL
            ORDER BY opponent_name
        """)
    return df["opponent_name"].tolist()


@st.cache_data(ttl=300)
def load_teams(season: str | None = None) -> list[str]:
    """Sorted list of team names for the filter widget."""
    if season:
        df = _q(f"""
            SELECT DISTINCT team_name
            FROM {_SCHEMA}.fantasy_match_points
            WHERE draft_position IS NOT NULL AND team_name IS NOT NULL AND season = %s
            ORDER BY team_name
        """, (season,))
    else:
        df = _q(f"""
            SELECT DISTINCT team_name
            FROM {_SCHEMA}.fantasy_match_points
            WHERE draft_position IS NOT NULL AND team_name IS NOT NULL
            ORDER BY team_name
        """)
    return df["team_name"].tolist()


@st.cache_data(ttl=300)
def load_leaderboard(
    season: str | None = None,
    positions: tuple | None = None,
    teams: tuple | None = None,
    opponents: tuple | None = None,
    side: str | None = None,
    start_date=None,
    end_date=None,
) -> pd.DataFrame:
    """Players ranked by total points, aggregated from fantasy_match_points
    so all three filters (position, team, home/away) work correctly."""
    clauses: list[str] = [
        "fmp.draft_position IS NOT NULL",
        "fmp.minutes_played > 0",
    ]
    params: list = []

    if season:
        clauses.append("fmp.season = %s")
        params.append(season)
    if start_date is not None:
        clauses.append("fmp.match_date >= %s::date")
        params.append(str(start_date))
    if end_date is not None:
        clauses.append("fmp.match_date <= %s::date")
        params.append(str(end_date))
    if positions:
        ph = ",".join(["%s"] * len(positions))
        clauses.append(f"fmp.draft_position IN ({ph})")
        params.extend(positions)
    if teams:
        ph = ",".join(["%s"] * len(teams))
        clauses.append(f"fmp.team_name IN ({ph})")
        params.extend(teams)
    if opponents:
        ph = ",".join(["%s"] * len(opponents))
        clauses.append(f"fmp.opponent_name IN ({ph})")
        params.extend(opponents)
    if side:
        clauses.append(
            "CASE WHEN fmp.team_id = dm.home_team_id THEN 'home' ELSE 'away' END = %s"
        )
        params.append(side)

    side_join = (
        f"LEFT JOIN {_SCHEMA}.dim_matches dm ON fmp.match_id = dm.match_id"
        if side else ""
    )
    where = "WHERE " + " AND ".join(clauses)

    return _q(f"""
        WITH base AS (
            SELECT
                fmp.player_name,
                fmp.draft_position,
                fmp.team_name,
                fmp.match_date,
                fmp.total_points
            FROM {_SCHEMA}.fantasy_match_points fmp
            {side_join}
            {where}
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY player_name ORDER BY match_date DESC
                ) AS recency
            FROM base
        )
        SELECT
            draft_position                                                      AS position,
            player_name,
            STRING_AGG(DISTINCT team_name, ' / ')
                FILTER (WHERE team_name IS NOT NULL)                           AS team_name,
            COUNT(*)                                                            AS matches_played,
            ROUND(SUM(total_points)::numeric, 1)                               AS total_pts,
            ROUND(AVG(total_points)::numeric, 2)                               AS avg_pts,
            ROUND(AVG(CASE WHEN recency <= 5 THEN total_points END)::numeric, 2) AS avg_pts_last5,
            ROUND((
                AVG(CASE WHEN recency <= 5 THEN total_points END) -
                AVG(total_points)
            )::numeric, 2)                                                     AS form_trend,
            MAX(total_points)                                                   AS season_high,
            MIN(total_points)                                                   AS season_low,
            ROUND(100.0 * SUM(CASE WHEN total_points > 0 THEN 1 ELSE 0 END)
                / COUNT(*), 1)                                                 AS consistency_pct
        FROM ranked
        GROUP BY 1, 2
        ORDER BY total_pts DESC NULLS LAST
    """, tuple(params) if params else None)


# ── Season lookup ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_seasons() -> pd.DataFrame:
    return _q(f"""
        SELECT DISTINCT season
        FROM {_SCHEMA}.fantasy_season_points
        ORDER BY season DESC
    """)


# ── Player queries ────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_player_list(season: str | None = None, team: str | None = None) -> pd.DataFrame:
    clauses = ["draft_position IS NOT NULL", "team_name IS NOT NULL"]
    params = []
    if season:
        clauses.append("season = %s")
        params.append(season)
    if team:
        clauses.append("team_name = %s")
        params.append(team)
    where = "WHERE " + " AND ".join(clauses)
    return _q(f"""
        SELECT DISTINCT player_name, draft_position
        FROM {_SCHEMA}.fantasy_match_points
        {where}
        ORDER BY player_name
    """, tuple(params) if params else None)


@st.cache_data(ttl=300)
def load_player_match_history(
    player_name: str,
    season: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    clauses = ["player_name = %s"]
    params = [player_name]
    if season:
        clauses.append("season = %s")
        params.append(season)
    if start_date:
        clauses.append("match_date >= %s::date")
        params.append(start_date)
    if end_date:
        clauses.append("match_date <= %s::date")
        params.append(end_date)
    where = "WHERE " + " AND ".join(clauses)
    return _q(f"""
        SELECT * FROM {_SCHEMA}.draft_points_per_match
        {where}
        ORDER BY season, match_number
    """, tuple(params))


@st.cache_data(ttl=300)
def load_player_season_totals(player_name: str, season: str | None = None) -> pd.DataFrame:
    if season:
        return _q(f"""
            SELECT * FROM {_SCHEMA}.fantasy_season_points
            WHERE player_name = %s AND season = %s
        """, (player_name, season))
    return _q(f"""
        SELECT * FROM {_SCHEMA}.fantasy_season_points
        WHERE player_name = %s
        ORDER BY season DESC
    """, (player_name,))


@st.cache_data(ttl=300)
def load_player_consistency(player_name: str, season: str | None = None) -> pd.DataFrame:
    if season:
        return _q(f"""
            SELECT * FROM {_SCHEMA}.draft_consistency
            WHERE player_name = %s AND season = %s
        """, (player_name, season))
    return _q(f"""
        SELECT * FROM {_SCHEMA}.draft_consistency
        WHERE player_name = %s
        ORDER BY season DESC
    """, (player_name,))


@st.cache_data(ttl=300)
def load_player_rank(player_name: str, season: str | None = None) -> pd.DataFrame:
    """League rank within position and rank on team among all positions."""
    season_clause = "AND season = %s" if season else ""
    # param slots: pos_counts(1) + player_league name(1) + player_league season(1) + team_totals(1)
    if season:
        params = (season, player_name, season, season)
    else:
        params = (player_name,)
    return _q(f"""
        WITH pos_counts AS (
            SELECT draft_position, season, COUNT(*) AS total_at_position
            FROM {_SCHEMA}.draft_rankings
            WHERE 1=1 {season_clause}
            GROUP BY draft_position, season
        ),
        player_league AS (
            SELECT r.player_name, r.draft_position, r.season,
                   r.position_rank, pc.total_at_position
            FROM {_SCHEMA}.draft_rankings r
            JOIN pos_counts pc
                ON r.draft_position = pc.draft_position AND r.season = pc.season
            WHERE r.player_name = %s {season_clause}
        ),
        team_totals AS (
            SELECT player_name, team_name, season,
                   SUM(total_points) AS total_points
            FROM {_SCHEMA}.fantasy_match_points
            WHERE draft_position IS NOT NULL {season_clause}
            GROUP BY player_name, team_name, season
        ),
        team_pts AS (
            SELECT
                player_name, team_name, season, total_points,
                RANK() OVER (
                    PARTITION BY team_name, season ORDER BY total_points DESC
                ) AS rank_on_team,
                COUNT(*) OVER (PARTITION BY team_name, season) AS total_on_team
            FROM team_totals
        ),
        best_team AS (
            SELECT DISTINCT ON (player_name, season)
                player_name, team_name, season, rank_on_team, total_on_team
            FROM team_pts
            ORDER BY player_name, season, total_points DESC
        )
        SELECT
            pl.position_rank,
            pl.total_at_position,
            bt.rank_on_team,
            bt.total_on_team,
            bt.team_name
        FROM player_league pl
        JOIN best_team bt
            ON pl.player_name = bt.player_name AND pl.season = bt.season
        LIMIT 1
    """, params)


@st.cache_data(ttl=300)
def load_player_goals_xg(player_name: str, season: str | None = None) -> pd.DataFrame:
    """Season goals and xG totals for the Goals vs xG chart."""
    clauses = ["player_name = %s", "minutes_played > 0"]
    params = [player_name]
    if season:
        clauses.append("season = %s")
        params.append(season)
    where = "WHERE " + " AND ".join(clauses)
    return _q(f"""
        SELECT
            COALESCE(SUM(goals), 0)                      AS total_goals,
            ROUND(COALESCE(SUM(xg), 0)::numeric, 2)      AS total_xg
        FROM {_SCHEMA}.player_match_stats
        {where}
    """, tuple(params))


@st.cache_data(ttl=300)
def load_player_position_stats(player_name: str, season: str | None = None) -> pd.DataFrame:
    if season:
        return _q(f"""
            SELECT * FROM {_SCHEMA}.draft_position_stats
            WHERE player_name = %s AND season = %s
        """, (player_name, season))
    return _q(f"""
        SELECT * FROM {_SCHEMA}.draft_position_stats
        WHERE player_name = %s
        ORDER BY season DESC
    """, (player_name,))


# ── Position queries ──────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_rankings_by_position(position: str, season: str | None = None) -> pd.DataFrame:
    """Draft rankings joined with consistency for scatter plot data."""
    if season:
        return _q(f"""
            SELECT r.*, c.avg_pts_last5, c.form_trend, c.form_rank
            FROM {_SCHEMA}.draft_rankings r
            LEFT JOIN {_SCHEMA}.draft_consistency c
                ON r.player_name = c.player_name
                AND r.draft_position = c.draft_position
                AND r.season = c.season
            WHERE r.draft_position = %s AND r.season = %s
            ORDER BY r.position_rank
        """, (position, season))
    return _q(f"""
        SELECT r.*, c.avg_pts_last5, c.form_trend, c.form_rank
        FROM {_SCHEMA}.draft_rankings r
        LEFT JOIN {_SCHEMA}.draft_consistency c
            ON r.player_name = c.player_name
            AND r.draft_position = c.draft_position
            AND r.season = c.season
        WHERE r.draft_position = %s
        ORDER BY r.season DESC, r.position_rank
    """, (position,))


@st.cache_data(ttl=300)
def load_consistency_by_position(position: str, season: str | None = None) -> pd.DataFrame:
    if season:
        return _q(f"""
            SELECT * FROM {_SCHEMA}.draft_consistency
            WHERE draft_position = %s AND season = %s
            ORDER BY form_rank
        """, (position, season))
    return _q(f"""
        SELECT * FROM {_SCHEMA}.draft_consistency
        WHERE draft_position = %s
        ORDER BY season DESC, form_rank
    """, (position,))


@st.cache_data(ttl=300)
def load_position_stats_by_position(position: str, season: str | None = None) -> pd.DataFrame:
    if season:
        return _q(f"""
            SELECT * FROM {_SCHEMA}.draft_position_stats
            WHERE draft_position = %s AND season = %s
            ORDER BY goal_contributions_rank
        """, (position, season))
    return _q(f"""
        SELECT * FROM {_SCHEMA}.draft_position_stats
        WHERE draft_position = %s
        ORDER BY season DESC, goal_contributions_rank
    """, (position,))


# ── Team queries ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_team_match_history(season: str | None = None) -> pd.DataFrame:
    """Self-join team_match_stats to derive goals_against and clean_sheet."""
    season_clause = "AND a.season = %s" if season else ""
    params = (season,) if season else None
    return _q(f"""
        SELECT
            a.match_id,
            a.match_date,
            a.season,
            a.team_id,
            a.team_name,
            a.side,
            a.opponent_name,
            a.goals                                           AS goals_for,
            b.goals                                           AS goals_against,
            CASE WHEN b.goals = 0 THEN TRUE ELSE FALSE END    AS clean_sheet,
            a.avg_player_rating,
            a.tackles,
            a.interceptions,
            a.clearances
        FROM {_SCHEMA}.team_match_stats a
        JOIN {_SCHEMA}.team_match_stats b
            ON a.match_id = b.match_id
            AND a.team_id != b.team_id
        WHERE 1=1 {season_clause}
        ORDER BY a.team_name, a.match_date
    """, params)


@st.cache_data(ttl=300)
def load_team_season_stats(season: str | None = None) -> pd.DataFrame:
    """team_season_stats enriched with clean_sheets and clean_sheet_pct."""
    season_clause = "AND a.season = %s" if season else ""
    season_filter = "AND s.season = %s" if season else ""
    params = (season, season) if season else None
    return _q(f"""
        WITH cs AS (
            SELECT
                a.team_id,
                a.team_name,
                a.season,
                SUM(CASE WHEN b.goals = 0 THEN 1 ELSE 0 END)               AS clean_sheets,
                COUNT(*)                                                     AS total_matches,
                ROUND(
                    SUM(CASE WHEN b.goals = 0 THEN 1 ELSE 0 END)::numeric
                    / NULLIF(COUNT(*), 0) * 100, 1
                )                                                            AS clean_sheet_pct
            FROM {_SCHEMA}.team_match_stats a
            JOIN {_SCHEMA}.team_match_stats b
                ON a.match_id = b.match_id AND a.team_id != b.team_id
            WHERE 1=1 {season_clause}
            GROUP BY a.team_id, a.team_name, a.season
        )
        SELECT s.*, cs.clean_sheets, cs.clean_sheet_pct
        FROM {_SCHEMA}.team_season_stats s
        LEFT JOIN cs ON s.team_id = cs.team_id AND s.season = cs.season
        WHERE 1=1 {season_filter}
    """, params)


@st.cache_data(ttl=300)
def load_player_targeting(positions=('GK', 'DF'), season: str | None = None) -> pd.DataFrame:
    """GK/DF players on top-6 clean-sheet teams."""
    pos_list = list(positions)
    placeholders = ','.join(['%s'] * len(pos_list))
    season_clause = "AND a.season = %s" if season else ""
    season_fmp = "AND draft_position IS NOT NULL AND season = %s" if season else "AND draft_position IS NOT NULL"
    season_dc = "AND dc.season = %s" if season else ""

    params: list = []
    if season:
        params.append(season)   # for cs_teams season_clause
    if season:
        params.append(season)   # for player_team season_fmp
    params.extend(pos_list)     # for IN clause
    if season:
        params.append(season)   # for dc season_dc

    return _q(f"""
        WITH cs_teams AS (
            SELECT
                a.team_name,
                SUM(CASE WHEN b.goals = 0 THEN 1 ELSE 0 END)::float
                    / NULLIF(COUNT(*), 0) * 100                  AS clean_sheet_pct
            FROM {_SCHEMA}.team_match_stats a
            JOIN {_SCHEMA}.team_match_stats b
                ON a.match_id = b.match_id AND a.team_id != b.team_id
            WHERE 1=1 {season_clause}
            GROUP BY a.team_name
            ORDER BY clean_sheet_pct DESC
            LIMIT 6
        ),
        player_team AS (
            SELECT DISTINCT player_name, team_name
            FROM {_SCHEMA}.fantasy_match_points
            WHERE {season_fmp[4:]}
        )
        SELECT
            dc.player_name,
            dc.draft_position,
            dc.avg_pts_last5,
            dc.form_trend,
            pt.team_name,
            ROUND(cs.clean_sheet_pct::numeric, 1) AS clean_sheet_pct
        FROM {_SCHEMA}.draft_consistency dc
        JOIN player_team pt ON dc.player_name = pt.player_name
        JOIN cs_teams cs ON pt.team_name = cs.team_name
        WHERE dc.draft_position IN ({placeholders}) {season_dc}
        ORDER BY dc.avg_pts_last5 DESC NULLS LAST
    """, tuple(params))
