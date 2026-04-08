#!/usr/bin/env python3
"""
Load Sofascore NWSL data into Neon Postgres (schema: sofascore).

Tables
------
  sofascore.dim_matches              match metadata (fetched live from Sofascore API)
  sofascore.fact_player_match_stats  per-player per-match stats (from nwsl_match_stats/ CSVs)

Usage
-----
  export DATABASE_URL="postgresql://..."
  python sofascore/load_to_db.py

The DATABASE_URL can be found in .streamlit/secrets.toml under [database].url.
"""

import os
import sys
import glob
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraping_script import build_events_df, SEASONS

STATS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nwsl_match_stats")


# ── Connection ────────────────────────────────────────────────────────────────

def get_conn():
    url = os.environ["DATABASE_URL"]
    # psycopg2 doesn't support channel_binding — strip it from the URL
    url = (url
           .replace("&channel_binding=require", "")
           .replace("?channel_binding=require&", "?")
           .replace("?channel_binding=require", ""))
    return psycopg2.connect(url)


# ── DDL ───────────────────────────────────────────────────────────────────────

DDL = """
CREATE SCHEMA IF NOT EXISTS sofascore;

CREATE TABLE IF NOT EXISTS sofascore.dim_matches (
    event_id            bigint          PRIMARY KEY,
    season              varchar(10)     NOT NULL,
    round               int,
    home_team           varchar(100)    NOT NULL,
    away_team           varchar(100)    NOT NULL,
    home_score          int,
    away_score          int,
    winner_code         int,
    status              varchar(30)     NOT NULL,
    start_timestamp     bigint,
    has_player_stats    boolean,
    match_date          date            NOT NULL
);

CREATE TABLE IF NOT EXISTS sofascore.fact_player_match_stats (
    event_id                        bigint          NOT NULL REFERENCES sofascore.dim_matches(event_id),
    player_id                       bigint          NOT NULL,
    season                          varchar(10),
    match_date                      date,
    home_team                       varchar(100),
    away_team                       varchar(100),
    team                            varchar(100),
    side                            varchar(10),
    player_name                     varchar(100),
    position                        varchar(5),
    substitute                      boolean,
    minutes_played                  float,
    rating                          float,
    rating_alternative              float,
    goals                           int,
    assists                         int,
    key_passes                      int,
    total_shots                     int,
    shots_on_target                 int,
    shots_off_target                int,
    shots_blocked                   int,
    big_chance_missed               int,
    total_offside                   int,
    total_pass                      int,
    accurate_pass                   int,
    total_long_balls                int,
    accurate_long_balls             int,
    total_cross                     int,
    accurate_cross                  int,
    own_half_passes                 int,
    accurate_own_half_passes        int,
    opp_half_passes                 int,
    accurate_opp_half_passes        int,
    carries_count                   int,
    carries_distance                float,
    progressive_carries_count       int,
    progressive_carries_distance    float,
    total_progression               float,
    best_carry_progression          float,
    touches                         int,
    unsuccessful_touch              int,
    possession_lost                 int,
    dispossessed                    int,
    duel_won                        int,
    duel_lost                       int,
    aerial_won                      int,
    aerial_lost                     int,
    total_contest                   int,
    won_contest                     int,
    challenge_lost                  int,
    total_tackle                    int,
    won_tackle                      int,
    interception_won                int,
    total_clearance                 int,
    ball_recovery                   int,
    fouls                           int,
    was_fouled                      int,
    shot_value                      float,
    pass_value                      float,
    dribble_value                   float,
    defensive_value                 float,
    PRIMARY KEY (event_id, player_id)
);
"""

# ── Column lists ──────────────────────────────────────────────────────────────

MATCH_COLS = [
    "event_id", "season", "round", "home_team", "away_team",
    "home_score", "away_score", "winner_code", "status",
    "start_timestamp", "has_player_stats", "match_date",
]

STATS_COLS = [
    "event_id", "player_id", "season", "match_date", "home_team", "away_team",
    "team", "side", "player_name", "position", "substitute", "minutes_played",
    "rating", "rating_alternative", "goals", "assists", "key_passes",
    "total_shots", "shots_on_target", "shots_off_target", "shots_blocked",
    "big_chance_missed", "total_offside", "total_pass", "accurate_pass",
    "total_long_balls", "accurate_long_balls", "total_cross", "accurate_cross",
    "own_half_passes", "accurate_own_half_passes", "opp_half_passes",
    "accurate_opp_half_passes", "carries_count", "carries_distance",
    "progressive_carries_count", "progressive_carries_distance",
    "total_progression", "best_carry_progression", "touches", "unsuccessful_touch",
    "possession_lost", "dispossessed", "duel_won", "duel_lost", "aerial_won",
    "aerial_lost", "total_contest", "won_contest", "challenge_lost",
    "total_tackle", "won_tackle", "interception_won", "total_clearance",
    "ball_recovery", "fouls", "was_fouled", "shot_value", "pass_value",
    "dribble_value", "defensive_value",
]

_STATS_NON_NUMERIC = {
    "event_id", "player_id", "season", "match_date",
    "home_team", "away_team", "team", "side", "player_name", "position",
}
STATS_NUMERIC_COLS = [c for c in STATS_COLS if c not in _STATS_NON_NUMERIC]


# ── Helpers ───────────────────────────────────────────────────────────────────

def to_rows(df, cols):
    """DataFrame → list of tuples, NaN → None, numpy scalars → Python natives."""
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


# ── Loaders ───────────────────────────────────────────────────────────────────

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    print("Schema and tables ready.")


def load_matches(conn):
    print("  Fetching match list from Sofascore API...")
    df = build_events_df(SEASONS)
    df = df.rename(columns={"date": "match_date"})

    for col in MATCH_COLS:
        if col not in df.columns:
            df[col] = None

    for col in ["event_id", "round", "home_score", "away_score", "winner_code", "start_timestamp"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["match_date"] = pd.to_datetime(df["match_date"]).dt.date
    df["has_player_stats"] = df["has_player_stats"].astype(bool)

    insert_cols = ", ".join(MATCH_COLS)
    update_set  = ", ".join(f"{c} = EXCLUDED.{c}" for c in MATCH_COLS[1:])

    with conn.cursor() as cur:
        execute_values(cur, f"""
            INSERT INTO sofascore.dim_matches ({insert_cols})
            VALUES %s
            ON CONFLICT (event_id) DO UPDATE SET {update_set}
        """, to_rows(df, MATCH_COLS))

    conn.commit()
    print(f"  Matches: {len(df)} rows upserted.")


def load_player_stats(conn):
    files = sorted(glob.glob(os.path.join(STATS_DIR, "*.csv")))
    if not files:
        print("  No stat CSV files found in nwsl_match_stats/.")
        return

    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception as e:
            print(f"  Warning: skipping {os.path.basename(f)}: {e}")

    if not dfs:
        return

    df = pd.concat(dfs, ignore_index=True)
    df = df.rename(columns={"date": "match_date"})

    for col in STATS_NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in STATS_COLS:
        if col not in df.columns:
            df[col] = None

    df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce").dt.date
    df["substitute"] = df["substitute"].map(
        {"True": True, "False": False, True: True, False: False}
    )

    # Skip rows whose event_id isn't yet in dim_matches (avoids FK violation)
    with conn.cursor() as cur:
        cur.execute("SELECT event_id FROM sofascore.dim_matches")
        known_ids = {row[0] for row in cur.fetchall()}

    unknown = set(df["event_id"].dropna().astype(int)) - known_ids
    if unknown:
        print(f"  Skipping {len(unknown)} event IDs not in dim_matches (e.g. {sorted(unknown)[:3]})")
        df = df[df["event_id"].astype(int).isin(known_ids)]

    insert_cols = ", ".join(STATS_COLS)
    update_set  = ", ".join(f"{c} = EXCLUDED.{c}" for c in STATS_COLS[2:])

    with conn.cursor() as cur:
        execute_values(cur, f"""
            INSERT INTO sofascore.fact_player_match_stats ({insert_cols})
            VALUES %s
            ON CONFLICT (event_id, player_id) DO UPDATE SET {update_set}
        """, to_rows(df, STATS_COLS))

    conn.commit()
    print(f"  Player stats: {len(df)} rows from {len(files)} files upserted.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print("Connecting to Neon Postgres...")
    conn = get_conn()
    try:
        print("Creating schema and tables...")
        create_tables(conn)
        print("Loading matches...")
        load_matches(conn)
        print("Loading player stats...")
        load_player_stats(conn)
        print("\nDone.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
