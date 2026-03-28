#!/usr/bin/env python3
"""
Load FotMob data into Neon Postgres (schema: fotmob).

Star schema
-----------
Dimensions
  fotmob.dim_teams        -- team_id, team_name
  fotmob.dim_players      -- player_id, player_name
  fotmob.dim_matches      -- match_id, home/away team FKs, utc_time, season, page_url

Facts
  fotmob.fact_lineups     -- per-player per-match formation/position/coordinates
  fotmob.fact_player_stats-- per-player per-match performance statistics (60+ cols)

Sources
  data/matches/2025.csv
  data/lineups/*.pkl      (CSV despite the .pkl extension)
  data/match_reports/*.csv

Requires env vars: NEON_HOST, NEON_USER, NEON_PASSWORD, NEON_DBNAME
Optional:          NEON_PORT (default 5432)

Usage
-----
  python fotmob/load_to_db.py
"""

import os
import glob
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_conn():
    return psycopg2.connect(
        host=os.environ["NEON_HOST"],
        user=os.environ["NEON_USER"],
        password=os.environ["NEON_PASSWORD"],
        dbname=os.environ["NEON_DBNAME"],
        port=int(os.environ.get("NEON_PORT", 5432)),
        sslmode="require",
    )


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL = """
CREATE SCHEMA IF NOT EXISTS fotmob;

-- ── Dimensions ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fotmob.dim_teams (
    team_id   INTEGER PRIMARY KEY,
    team_name TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS fotmob.dim_players (
    player_id   INTEGER PRIMARY KEY,
    player_name TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS fotmob.dim_matches (
    match_id     INTEGER PRIMARY KEY,
    home_team_id INTEGER REFERENCES fotmob.dim_teams(team_id),
    away_team_id INTEGER REFERENCES fotmob.dim_teams(team_id),
    utc_time     TIMESTAMPTZ,
    season       TEXT,
    page_url     TEXT
);

-- ── Facts ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fotmob.fact_lineups (
    id                BIGSERIAL PRIMARY KEY,
    match_id          INTEGER   NOT NULL REFERENCES fotmob.dim_matches(match_id),
    player_id         INTEGER   NOT NULL REFERENCES fotmob.dim_players(player_id),
    team_id           INTEGER   NOT NULL REFERENCES fotmob.dim_teams(team_id),
    side              TEXT,                  -- 'home' | 'away'
    bucket            TEXT,                  -- 'starters' | 'bench'
    formation         TEXT,
    shirt_number      SMALLINT,
    position_id       SMALLINT,
    usual_position_id SMALLINT,
    rating            NUMERIC(4,2),
    h_x               NUMERIC(6,4),
    h_y               NUMERIC(6,4),
    v_x               NUMERIC(6,4),
    v_y               NUMERIC(6,4),
    UNIQUE (match_id, player_id)
);

CREATE TABLE IF NOT EXISTS fotmob.fact_player_stats (
    id                            BIGSERIAL PRIMARY KEY,
    match_id                      INTEGER   NOT NULL REFERENCES fotmob.dim_matches(match_id),
    player_id                     INTEGER   NOT NULL REFERENCES fotmob.dim_players(player_id),
    href                          TEXT,
    fotmob_rating                 NUMERIC(5,2),
    minutes_played                SMALLINT,
    goals                         SMALLINT,
    assists                       SMALLINT,
    xg                            NUMERIC(6,3),
    xa                            NUMERIC(6,3),
    xg_plus_xa                    NUMERIC(6,3),
    defensive_contributions       SMALLINT,
    xgot                          NUMERIC(6,3),
    shots_on_target               SMALLINT,
    touches_in_opposition_box     SMALLINT,
    offsides                      SMALLINT,
    dispossessed                  SMALLINT,
    touches                       SMALLINT,
    chances_created               SMALLINT,
    passes_into_final_third       SMALLINT,
    tackles                       SMALLINT,
    interceptions                 SMALLINT,
    blocks                        SMALLINT,
    recoveries                    SMALLINT,
    clearances                    SMALLINT,
    headed_clearance              SMALLINT,
    dribbled_past                 SMALLINT,
    duels_won                     SMALLINT,
    duels_lost                    SMALLINT,
    fouls_committed               SMALLINT,
    was_fouled                    SMALLINT,
    saves                         SMALLINT,
    goals_conceded                SMALLINT,
    xgot_faced                    NUMERIC(6,3),
    goals_prevented               NUMERIC(6,3),
    acted_as_sweeper              SMALLINT,
    high_claim                    SMALLINT,
    -- fraction breakdown cols (pre-split from "X/Y (Z%)" strings)
    successful_dribbles_succeeded SMALLINT,
    successful_dribbles_attempted SMALLINT,
    successful_dribbles_pct       NUMERIC(5,4),
    accurate_passes_succeeded     SMALLINT,
    accurate_passes_attempted     SMALLINT,
    accurate_passes_pct           NUMERIC(5,4),
    accurate_crosses_succeeded    SMALLINT,
    accurate_crosses_attempted    SMALLINT,
    accurate_crosses_pct          NUMERIC(5,4),
    accurate_long_balls_succeeded SMALLINT,
    accurate_long_balls_attempted SMALLINT,
    accurate_long_balls_pct       NUMERIC(5,4),
    ground_duels_won_succeeded    SMALLINT,
    ground_duels_won_attempted    SMALLINT,
    ground_duels_won_pct          NUMERIC(5,4),
    aerial_duels_won_succeeded    SMALLINT,
    aerial_duels_won_attempted    SMALLINT,
    aerial_duels_won_pct          NUMERIC(5,4),
    UNIQUE (match_id, player_id)
);
"""

# ---------------------------------------------------------------------------
# Column lists (order must match VALUES %s tuples)
# ---------------------------------------------------------------------------

LINEUP_COLS = [
    "match_id", "player_id", "team_id", "side", "bucket", "formation",
    "shirt_number", "position_id", "usual_position_id", "rating",
    "h_x", "h_y", "v_x", "v_y",
]

STATS_COLS = [
    "match_id", "player_id", "href", "fotmob_rating", "minutes_played",
    "goals", "assists", "xg", "xa", "xg_plus_xa", "defensive_contributions",
    "xgot", "shots_on_target", "touches_in_opposition_box", "offsides",
    "dispossessed", "touches", "chances_created", "passes_into_final_third",
    "tackles", "interceptions", "blocks", "recoveries", "clearances",
    "headed_clearance", "dribbled_past", "duels_won", "duels_lost",
    "fouls_committed", "was_fouled", "saves", "goals_conceded",
    "xgot_faced", "goals_prevented", "acted_as_sweeper", "high_claim",
    "successful_dribbles_succeeded", "successful_dribbles_attempted", "successful_dribbles_pct",
    "accurate_passes_succeeded", "accurate_passes_attempted", "accurate_passes_pct",
    "accurate_crosses_succeeded", "accurate_crosses_attempted", "accurate_crosses_pct",
    "accurate_long_balls_succeeded", "accurate_long_balls_attempted", "accurate_long_balls_pct",
    "ground_duels_won_succeeded", "ground_duels_won_attempted", "ground_duels_won_pct",
    "aerial_duels_won_succeeded", "aerial_duels_won_attempted", "aerial_duels_won_pct",
]

STATS_NUMERIC_COLS = [c for c in STATS_COLS if c not in ("match_id", "player_id", "href")]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def to_rows(df, cols):
    """Convert a DataFrame to a list of tuples, replacing NaN/NA with None
    and coercing numpy scalar types to native Python types for psycopg2."""
    def convert(v):
        if pd.isna(v):
            return None
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, np.floating):
            return float(v)
        return v

    return [
        tuple(convert(v) for v in row)
        for row in df[cols].itertuples(index=False, name=None)
    ]


def load_active_match_ids():
    """Return a set of match IDs from data/active_match_ids.txt, or None if file doesn't exist."""
    path = os.path.join(DATA_DIR, "active_match_ids.txt")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        ids = {int(line.strip()) for line in f if line.strip().isdigit()}
    print(f"  Filtering to {len(ids)} active match IDs from active_match_ids.txt")
    return ids


def read_csvs(pattern, skip_non_numeric_names=False, match_ids=None):
    """Glob files, optionally skip files whose stem is not a match_id integer,
    and optionally filter to a specific set of match IDs."""
    files = sorted(glob.glob(pattern))
    if skip_non_numeric_names:
        files = [f for f in files if os.path.splitext(os.path.basename(f))[0].isdigit()]
    if match_ids is not None:
        files = [f for f in files if int(os.path.splitext(os.path.basename(f))[0]) in match_ids]
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception as e:
            print(f"  Warning: skipping {os.path.basename(f)}: {e}")
    return dfs, len(files)


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    print("Schema and tables ready.")


def load_matches(conn):
    """Load dim_teams and dim_matches from all CSVs in data/matches/.
    Season is inferred from the filename stem (e.g. 2025.csv → '2025').
    """
    csv_files = sorted(glob.glob(os.path.join(DATA_DIR, "matches", "*.csv")))
    if not csv_files:
        print("  No match CSV files found.")
        return

    all_dfs = []
    for path in csv_files:
        season = os.path.splitext(os.path.basename(path))[0]
        df = pd.read_csv(path)
        df["season"] = season
        all_dfs.append(df)

    df = pd.concat(all_dfs, ignore_index=True)

    teams = pd.concat([
        df[["home_team_id", "home_team"]].rename(columns={"home_team_id": "team_id", "home_team": "team_name"}),
        df[["away_team_id", "away_team"]].rename(columns={"away_team_id": "team_id", "away_team": "team_name"}),
    ]).drop_duplicates("team_id").reset_index(drop=True)
    teams["team_name"] = teams["team_name"].str.replace(r"\s*\(W\)\s*$", "", regex=True).str.strip()

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO fotmob.dim_teams (team_id, team_name)
            VALUES %s
            ON CONFLICT (team_id) DO UPDATE SET team_name = EXCLUDED.team_name
        """, list(teams.itertuples(index=False, name=None)))

    df["utc_time"] = pd.to_datetime(df["utc_time"], utc=True, errors="coerce")
    df = df.rename(columns={"pageUrl": "page_url"})
    if "page_url" not in df.columns:
        df["page_url"] = None

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO fotmob.dim_matches (match_id, home_team_id, away_team_id, utc_time, season, page_url)
            VALUES %s
            ON CONFLICT (match_id) DO UPDATE SET
                home_team_id = EXCLUDED.home_team_id,
                away_team_id = EXCLUDED.away_team_id,
                utc_time     = EXCLUDED.utc_time,
                season       = EXCLUDED.season,
                page_url     = EXCLUDED.page_url
        """, to_rows(df, ["match_id", "home_team_id", "away_team_id", "utc_time", "season", "page_url"]))

    conn.commit()
    print(f"  Matches: {len(teams)} teams, {len(df)} matches across {len(csv_files)} season file(s)")


def load_lineups(conn, match_ids=None):
    """Load dim_players (partial) and fact_lineups from data/lineups/*.pkl."""
    all_files = sorted(glob.glob(os.path.join(DATA_DIR, "lineups", "*.pkl")))
    files = [f for f in all_files
             if match_ids is None or int(os.path.splitext(os.path.basename(f))[0]) in match_ids]
    if not files:
        print("  No lineup files found.")
        return

    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_pickle(f))
        except Exception:
            try:
                dfs.append(pd.read_csv(f))
            except Exception as e:
                print(f"  Warning: skipping {os.path.basename(f)}: {e}")

    if not dfs:
        print("  No lineup files could be read.")
        return

    n_files = len(files)
    df = pd.concat(dfs, ignore_index=True)

    # Upsert players discovered in lineups
    players = df[["player_id", "player_name"]].drop_duplicates("player_id")
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO fotmob.dim_players (player_id, player_name)
            VALUES %s
            ON CONFLICT (player_id) DO UPDATE SET player_name = EXCLUDED.player_name
        """, list(players.itertuples(index=False, name=None)))

    # Drop rows for match IDs not yet in dim_matches (avoids FK violation)
    with conn.cursor() as cur:
        cur.execute("SELECT match_id FROM fotmob.dim_matches")
        known_ids = {row[0] for row in cur.fetchall()}
    unknown = set(df["match_id"].dropna().astype(int)) - known_ids
    if unknown:
        print(f"  Skipping {len(unknown)} match IDs not in dim_matches (e.g. {sorted(unknown)[:3]})")
        df = df[df["match_id"].astype(int).isin(known_ids)]

    for col in ["shirt_number", "position_id", "usual_position_id"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ["rating", "h_x", "h_y", "v_x", "v_y"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO fotmob.fact_lineups
                (match_id, player_id, team_id, side, bucket, formation,
                 shirt_number, position_id, usual_position_id, rating,
                 h_x, h_y, v_x, v_y)
            VALUES %s
            ON CONFLICT (match_id, player_id) DO UPDATE SET
                team_id           = EXCLUDED.team_id,
                side              = EXCLUDED.side,
                bucket            = EXCLUDED.bucket,
                formation         = EXCLUDED.formation,
                shirt_number      = EXCLUDED.shirt_number,
                position_id       = EXCLUDED.position_id,
                usual_position_id = EXCLUDED.usual_position_id,
                rating            = EXCLUDED.rating,
                h_x               = EXCLUDED.h_x,
                h_y               = EXCLUDED.h_y,
                v_x               = EXCLUDED.v_x,
                v_y               = EXCLUDED.v_y
        """, to_rows(df, LINEUP_COLS))

    conn.commit()
    print(f"  Lineups:  {len(df)} rows from {n_files} files")


def load_player_stats(conn, match_ids=None):
    """Load dim_players (remainder) and fact_player_stats from data/match_reports/*.csv."""
    # skip_non_numeric_names filters out duplicate_metric_report_all_matches.csv etc.
    dfs, n_files = read_csvs(
        os.path.join(DATA_DIR, "match_reports", "*.csv"),
        skip_non_numeric_names=True,
        match_ids=match_ids,
    )
    if not dfs:
        print("  No match report files found.")
        return

    df = pd.concat(dfs, ignore_index=True)

    # Upsert any players not already loaded from lineups
    players = df[["player_id", "player_name"]].drop_duplicates("player_id")
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO fotmob.dim_players (player_id, player_name)
            VALUES %s
            ON CONFLICT (player_id) DO UPDATE SET player_name = EXCLUDED.player_name
        """, list(players.itertuples(index=False, name=None)))

    # Drop rows for match IDs not yet in dim_matches (avoids FK violation)
    with conn.cursor() as cur:
        cur.execute("SELECT match_id FROM fotmob.dim_matches")
        known_ids = {row[0] for row in cur.fetchall()}
    unknown = set(df["match_id"].dropna().astype(int)) - known_ids
    if unknown:
        print(f"  Skipping {len(unknown)} match IDs not in dim_matches (e.g. {sorted(unknown)[:3]})")
        df = df[df["match_id"].astype(int).isin(known_ids)]

    # Coerce all numeric stat columns; fraction strings ("2/3 (67%)") become NaN
    for col in STATS_NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ensure every expected column exists (add as NaN if missing)
    for col in STATS_COLS:
        if col not in df.columns:
            df[col] = None

    insert_cols = ", ".join(STATS_COLS)
    update_set  = ", ".join(f"{c} = EXCLUDED.{c}" for c in STATS_COLS[2:])  # skip match_id, player_id

    with conn.cursor() as cur:
        execute_values(cur, f"""
            INSERT INTO fotmob.fact_player_stats ({insert_cols})
            VALUES %s
            ON CONFLICT (match_id, player_id) DO UPDATE SET {update_set}
        """, to_rows(df, STATS_COLS))

    conn.commit()
    print(f"  Stats:    {len(df)} rows from {n_files} files")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print("Connecting to Neon Postgres...")
    conn = get_conn()
    try:
        print("Creating schema and tables...")
        create_tables(conn)
        print("Loading matches and teams...")
        load_matches(conn)
        match_ids = load_active_match_ids()
        print("Loading lineups...")
        load_lineups(conn, match_ids=match_ids)
        print("Loading player stats...")
        load_player_stats(conn, match_ids=match_ids)
        print("\nDone.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
