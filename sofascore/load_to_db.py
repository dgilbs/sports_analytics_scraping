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
import tomllib
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Auto-load .envrc so NEON_* vars are available without sourcing manually
_envrc = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".envrc")
if os.path.exists(_envrc):
    with open(_envrc) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line.startswith("export "):
                _line = _line[len("export "):]
            if "=" in _line and not _line.startswith("#"):
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip().strip('"\''))

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraping_script import build_events_df, SEASONS

STATS_DIR     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nwsl_match_stats")
SHOTS_DIR     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nwsl_shot_tables")
PASS_MAP_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nwsl_passmap_data")
DRIB_MAP_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nwsl_dribmap_data")
DEF_MAP_DIR   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nwsl_defmap_data")


# ── Connection ────────────────────────────────────────────────────────────────

def get_conn():
    """Connect using NEON_* env vars (same as fotmob/dbt), or fall back to secrets.toml URL."""
    if "NEON_HOST" in os.environ:
        return psycopg2.connect(
            host=os.environ["NEON_HOST"],
            user=os.environ["NEON_USER"],
            password=os.environ["NEON_PASSWORD"],
            dbname=os.environ["NEON_DBNAME"],
            port=int(os.environ.get("NEON_PORT", 5432)),
            sslmode="require",
        )
    # Fall back to secrets.toml
    secrets_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", ".streamlit", "secrets.toml"
    )
    with open(secrets_path, "rb") as f:
        secrets = tomllib.load(f)
    url = "".join(secrets["database"]["url"].split())
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

CREATE TABLE IF NOT EXISTS sofascore.fact_shots (
    id                  bigserial       PRIMARY KEY,
    event_id            bigint          NOT NULL REFERENCES sofascore.dim_matches(event_id),
    season              varchar(10),
    match_date          date,
    home_team           varchar(100),
    away_team           varchar(100),
    team                varchar(100),
    side                varchar(10),
    player_id           bigint,
    player_name         varchar(100),
    position            varchar(10),
    shot_type           varchar(20),
    situation           varchar(30),
    body_part           varchar(20),
    goal_mouth_location varchar(30),
    time                int,
    added_time          int,
    time_seconds        int,
    player_x            float,
    player_y            float,
    goal_mouth_x        float,
    goal_mouth_y        float,
    goal_mouth_z        float,
    draw_start_x        float,
    draw_start_y        float,
    draw_end_x          float,
    draw_end_y          float,
    is_goal             boolean,
    is_on_target        boolean,
    is_blocked          boolean
);

CREATE TABLE IF NOT EXISTS sofascore.fact_pass_maps (
    event_id                bigint      NOT NULL REFERENCES sofascore.dim_matches(event_id),
    player_id               bigint      NOT NULL,
    season                  varchar(10),
    match_date              date,
    home_team               varchar(100),
    away_team               varchar(100),
    team                    varchar(100),
    side                    varchar(10),
    player_name             varchar(100),
    position                varchar(5),
    substitute              boolean,
    passes_total            int,
    passes_accurate         int,
    passes_inaccurate       int,
    pass_accuracy           float,
    avg_pass_length         float,
    pct_forward             float,
    pct_backward            float,
    pct_lateral             float,
    acc_pct_forward         float,
    acc_pct_backward        float,
    acc_pct_lateral         float,
    origin_def_third        float,
    origin_mid_third        float,
    origin_att_third        float,
    origin_left_wing        float,
    origin_central          float,
    origin_right_wing       float,
    dest_def_third          float,
    dest_mid_third          float,
    dest_att_third          float,
    dest_left_wing          float,
    dest_central            float,
    dest_right_wing         float,
    acc_dest_def_third      float,
    acc_dest_mid_third      float,
    acc_dest_att_third      float,
    acc_dest_left_wing      float,
    acc_dest_central        float,
    acc_dest_right_wing     float,
    progressive_passes              int,
    progressive_pass_pct            float,
    passes_into_final_third         int,
    acc_passes_into_final_third     int,
    passes_into_penalty_area        int,
    acc_passes_into_penalty_area    int,
    crosses_into_penalty_area       int,
    passes_short                    int,
    passes_medium                   int,
    passes_long                     int,
    acc_passes_short                int,
    acc_passes_medium               int,
    acc_passes_long                 int,
    PRIMARY KEY (event_id, player_id)
);

ALTER TABLE sofascore.fact_pass_maps ADD COLUMN IF NOT EXISTS passes_into_final_third         int;
ALTER TABLE sofascore.fact_pass_maps ADD COLUMN IF NOT EXISTS acc_passes_into_final_third     int;
ALTER TABLE sofascore.fact_pass_maps ADD COLUMN IF NOT EXISTS passes_into_penalty_area        int;
ALTER TABLE sofascore.fact_pass_maps ADD COLUMN IF NOT EXISTS acc_passes_into_penalty_area    int;
ALTER TABLE sofascore.fact_pass_maps ADD COLUMN IF NOT EXISTS crosses_into_penalty_area       int;
ALTER TABLE sofascore.fact_pass_maps ADD COLUMN IF NOT EXISTS passes_short                    int;
ALTER TABLE sofascore.fact_pass_maps ADD COLUMN IF NOT EXISTS passes_medium                   int;
ALTER TABLE sofascore.fact_pass_maps ADD COLUMN IF NOT EXISTS passes_long                     int;
ALTER TABLE sofascore.fact_pass_maps ADD COLUMN IF NOT EXISTS acc_passes_short                int;
ALTER TABLE sofascore.fact_pass_maps ADD COLUMN IF NOT EXISTS acc_passes_medium               int;
ALTER TABLE sofascore.fact_pass_maps ADD COLUMN IF NOT EXISTS acc_passes_long                 int;

CREATE TABLE IF NOT EXISTS sofascore.fact_drib_maps (
    event_id                bigint      NOT NULL REFERENCES sofascore.dim_matches(event_id),
    player_id               bigint      NOT NULL,
    season                  varchar(10),
    match_date              date,
    home_team               varchar(100),
    away_team               varchar(100),
    team                    varchar(100),
    side                    varchar(10),
    player_name             varchar(100),
    position                varchar(5),
    substitute              boolean,
    dribbles_won            int,
    dribbles_lost           int,
    dribbles_total          int,
    dribble_success         float,
    carry_segments          int,
    drib_def_third          float,
    drib_mid_third          float,
    drib_att_third          float,
    drib_left_wing          float,
    drib_central            float,
    drib_right_wing         float,
    carry_def_third         float,
    carry_mid_third         float,
    carry_att_third         float,
    carry_left_wing         float,
    carry_central           float,
    carry_right_wing        float,
    drib_won_def_third          float,
    drib_won_mid_third          float,
    drib_won_att_third          float,
    carries_into_final_third    int,
    carries_into_penalty_area   int,
    PRIMARY KEY (event_id, player_id)
);

ALTER TABLE sofascore.fact_drib_maps ADD COLUMN IF NOT EXISTS carries_into_final_third  int;
ALTER TABLE sofascore.fact_drib_maps ADD COLUMN IF NOT EXISTS carries_into_penalty_area int;

CREATE TABLE IF NOT EXISTS sofascore.fact_def_maps (
    event_id                bigint      NOT NULL REFERENCES sofascore.dim_matches(event_id),
    player_id               bigint      NOT NULL,
    season                  varchar(10),
    match_date              date,
    home_team               varchar(100),
    away_team               varchar(100),
    team                    varchar(100),
    side                    varchar(10),
    player_name             varchar(100),
    position                varchar(5),
    substitute              boolean,
    tackle_won              int,
    missed_tackle           int,
    interception            int,
    clearance               int,
    block                   int,
    recovery                int,
    total_def_actions       int,
    tackle_success          float,
    pct_def_third           float,
    pct_mid_third           float,
    pct_att_third           float,
    pct_left_wing           float,
    pct_central             float,
    pct_right_wing          float,
    tackle_def_third        float,
    tackle_mid_third        float,
    tackle_att_third        float,
    intercept_def_third     float,
    intercept_mid_third     float,
    intercept_att_third     float,
    recovery_def_third      float,
    recovery_mid_third      float,
    recovery_att_third      float,
    PRIMARY KEY (event_id, player_id)
);

CREATE TABLE IF NOT EXISTS sofascore.fact_heatmaps (
    event_id            bigint          NOT NULL REFERENCES sofascore.dim_matches(event_id),
    player_id           bigint          NOT NULL,
    season              varchar(10),
    match_date          date,
    home_team           varchar(100),
    away_team           varchar(100),
    team                varchar(100),
    side                varchar(10),
    player_name         varchar(100),
    position            varchar(5),
    substitute          boolean,
    touch_count         int,
    defensive_third     float,
    middle_third        float,
    attacking_third     float,
    left_wing           float,
    central             float,
    right_wing          float,
    att_penalty_area    float,
    PRIMARY KEY (event_id, player_id)
);
ALTER TABLE sofascore.fact_heatmaps ADD COLUMN IF NOT EXISTS att_penalty_area float;
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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _event_files(data_dir, event_ids=None):
    """Return CSV file paths for a directory, optionally filtered to specific event IDs."""
    if event_ids is not None:
        files = [os.path.join(data_dir, f"{eid}.csv") for eid in event_ids
                 if os.path.exists(os.path.join(data_dir, f"{eid}.csv"))]
        return sorted(files)
    return sorted(glob.glob(os.path.join(data_dir, "*.csv")))


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


def load_player_stats(conn, event_ids=None):
    files = _event_files(STATS_DIR, event_ids)
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


SHOTS_COLS = [
    "event_id", "season", "match_date", "home_team", "away_team",
    "team", "side", "player_id", "player_name", "position",
    "shot_type", "situation", "body_part", "goal_mouth_location",
    "time", "added_time", "time_seconds",
    "player_x", "player_y",
    "goal_mouth_x", "goal_mouth_y", "goal_mouth_z",
    "draw_start_x", "draw_start_y", "draw_end_x", "draw_end_y",
    "is_goal", "is_on_target", "is_blocked",
]

SHOTS_NUMERIC_COLS = [
    "event_id", "player_id", "time", "added_time", "time_seconds",
    "player_x", "player_y", "goal_mouth_x", "goal_mouth_y", "goal_mouth_z",
    "draw_start_x", "draw_start_y", "draw_end_x", "draw_end_y",
]


def load_shots(conn, event_ids=None):
    files = _event_files(SHOTS_DIR, event_ids)
    if not files:
        print("  No shot CSV files found in nwsl_shot_tables/.")
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

    for col in SHOTS_NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["is_goal", "is_on_target", "is_blocked"]:
        df[col] = df[col].map({"True": True, "False": False, True: True, False: False})

    df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce").dt.date

    for col in SHOTS_COLS:
        if col not in df.columns:
            df[col] = None

    # Skip rows whose event_id isn't yet in dim_matches
    with conn.cursor() as cur:
        cur.execute("SELECT event_id FROM sofascore.dim_matches")
        known_ids = {row[0] for row in cur.fetchall()}

    unknown = set(df["event_id"].dropna().astype(int)) - known_ids
    if unknown:
        print(f"  Skipping {len(unknown)} event IDs not in dim_matches (e.g. {sorted(unknown)[:3]})")
        df = df[df["event_id"].astype(int).isin(known_ids)]

    # Replace all shots for each event to avoid duplicates (no stable shot ID)
    event_ids = df["event_id"].dropna().unique().tolist()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM sofascore.fact_shots WHERE event_id = ANY(%s)",
            (event_ids,)
        )

    insert_cols = ", ".join(SHOTS_COLS)
    with conn.cursor() as cur:
        execute_values(cur, f"""
            INSERT INTO sofascore.fact_shots ({insert_cols})
            VALUES %s
        """, to_rows(df, SHOTS_COLS))

    conn.commit()
    print(f"  Shots: {len(df)} rows from {len(files)} files loaded.")


PASS_MAP_COLS = [
    "event_id", "player_id", "season", "match_date", "home_team", "away_team",
    "team", "side", "player_name", "position", "substitute",
    "passes_total", "passes_accurate", "passes_inaccurate", "pass_accuracy",
    "avg_pass_length", "pct_forward", "pct_backward", "pct_lateral",
    "acc_pct_forward", "acc_pct_backward", "acc_pct_lateral",
    "origin_def_third", "origin_mid_third", "origin_att_third",
    "origin_left_wing", "origin_central", "origin_right_wing",
    "dest_def_third", "dest_mid_third", "dest_att_third",
    "dest_left_wing", "dest_central", "dest_right_wing",
    "acc_dest_def_third", "acc_dest_mid_third", "acc_dest_att_third",
    "acc_dest_left_wing", "acc_dest_central", "acc_dest_right_wing",
    "progressive_passes", "progressive_pass_pct",
    "passes_into_final_third", "acc_passes_into_final_third",
    "passes_into_penalty_area", "acc_passes_into_penalty_area",
    "crosses_into_penalty_area",
    "passes_short", "passes_medium", "passes_long",
    "acc_passes_short", "acc_passes_medium", "acc_passes_long",
]

DRIB_MAP_COLS = [
    "event_id", "player_id", "season", "match_date", "home_team", "away_team",
    "team", "side", "player_name", "position", "substitute",
    "dribbles_won", "dribbles_lost", "dribbles_total", "dribble_success",
    "carry_segments",
    "drib_def_third", "drib_mid_third", "drib_att_third",
    "drib_left_wing", "drib_central", "drib_right_wing",
    "carry_def_third", "carry_mid_third", "carry_att_third",
    "carry_left_wing", "carry_central", "carry_right_wing",
    "drib_won_def_third", "drib_won_mid_third", "drib_won_att_third",
    "carries_into_final_third", "carries_into_penalty_area",
]

DEF_MAP_COLS = [
    "event_id", "player_id", "season", "match_date", "home_team", "away_team",
    "team", "side", "player_name", "position", "substitute",
    "tackle_won", "missed_tackle", "interception", "clearance", "block",
    "recovery", "total_def_actions", "tackle_success",
    "pct_def_third", "pct_mid_third", "pct_att_third",
    "pct_left_wing", "pct_central", "pct_right_wing",
    "tackle_def_third", "tackle_mid_third", "tackle_att_third",
    "intercept_def_third", "intercept_mid_third", "intercept_att_third",
    "recovery_def_third", "recovery_mid_third", "recovery_att_third",
]

_MAP_NON_NUMERIC = {
    "event_id", "player_id", "season", "match_date",
    "home_team", "away_team", "team", "side", "player_name", "position",
}


def _load_map_table(conn, data_dir, cols, table_name, label, event_ids=None):
    files = _event_files(data_dir, event_ids)
    if not files:
        print(f"  No CSV files found in {os.path.basename(data_dir)}/.")
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

    for col in [c for c in cols if c not in _MAP_NON_NUMERIC and c != "substitute"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce").dt.date
    df["substitute"] = df["substitute"].map(
        {"True": True, "False": False, True: True, False: False}
    )

    for col in cols:
        if col not in df.columns:
            df[col] = None

    with conn.cursor() as cur:
        cur.execute("SELECT event_id FROM sofascore.dim_matches")
        known_ids = {row[0] for row in cur.fetchall()}

    unknown = set(df["event_id"].dropna().astype(int)) - known_ids
    if unknown:
        print(f"  Skipping {len(unknown)} event IDs not in dim_matches")
        df = df[df["event_id"].astype(int).isin(known_ids)]

    insert_cols = ", ".join(cols)
    update_set  = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols[2:])

    with conn.cursor() as cur:
        execute_values(cur, f"""
            INSERT INTO {table_name} ({insert_cols})
            VALUES %s
            ON CONFLICT (event_id, player_id) DO UPDATE SET {update_set}
        """, to_rows(df, cols))

    conn.commit()
    print(f"  {label}: {len(df)} rows from {len(files)} files upserted.")


def load_pass_maps(conn, event_ids=None):
    _load_map_table(conn, PASS_MAP_DIR, PASS_MAP_COLS, "sofascore.fact_pass_maps", "Pass maps", event_ids)


def load_drib_maps(conn, event_ids=None):
    _load_map_table(conn, DRIB_MAP_DIR, DRIB_MAP_COLS, "sofascore.fact_drib_maps", "Drib maps", event_ids)


def load_def_maps(conn, event_ids=None):
    _load_map_table(conn, DEF_MAP_DIR, DEF_MAP_COLS, "sofascore.fact_def_maps", "Def maps", event_ids)


HEATMAP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nwsl_heatmap_zones")

HEATMAP_COLS = [
    "event_id", "player_id", "season", "match_date", "home_team", "away_team",
    "team", "side", "player_name", "position", "substitute",
    "touch_count",
    "defensive_third", "middle_third", "attacking_third",
    "left_wing", "central", "right_wing",
    "att_penalty_area",
]

_HEATMAP_NON_NUMERIC = {
    "event_id", "player_id", "season", "match_date",
    "home_team", "away_team", "team", "side", "player_name", "position",
}


def load_heatmaps(conn, event_ids=None):
    files = _event_files(HEATMAP_DIR, event_ids)
    if not files:
        print("  No heatmap CSV files found in nwsl_heatmap_zones/.")
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

    for col in [c for c in HEATMAP_COLS if c not in _HEATMAP_NON_NUMERIC and c != "substitute"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce").dt.date
    df["substitute"] = df["substitute"].map(
        {"True": True, "False": False, True: True, False: False}
    )

    for col in HEATMAP_COLS:
        if col not in df.columns:
            df[col] = None

    with conn.cursor() as cur:
        cur.execute("SELECT event_id FROM sofascore.dim_matches")
        known_ids = {row[0] for row in cur.fetchall()}

    unknown = set(df["event_id"].dropna().astype(int)) - known_ids
    if unknown:
        print(f"  Skipping {len(unknown)} event IDs not in dim_matches")
        df = df[df["event_id"].astype(int).isin(known_ids)]

    insert_cols = ", ".join(HEATMAP_COLS)
    update_set  = ", ".join(f"{c} = EXCLUDED.{c}" for c in HEATMAP_COLS[2:])

    with conn.cursor() as cur:
        execute_values(cur, f"""
            INSERT INTO sofascore.fact_heatmaps ({insert_cols})
            VALUES %s
            ON CONFLICT (event_id, player_id) DO UPDATE SET {update_set}
        """, to_rows(df, HEATMAP_COLS))

    conn.commit()
    print(f"  Heatmaps: {len(df)} rows from {len(files)} files upserted.")


def load_for_events(conn, event_ids):
    """Load all data tables for a specific list of event IDs only."""
    ids = [int(e) for e in event_ids]
    load_player_stats(conn, ids)
    load_shots(conn, ids)
    load_pass_maps(conn, ids)
    load_drib_maps(conn, ids)
    load_def_maps(conn, ids)
    load_heatmaps(conn, ids)


def validate_maps(conn, event_ids=None):
    """
    Flag suspicious zone assignments that likely indicate a data error.

    Checks:
      1. Defender (D) with 0% actions in defensive third — wrong orientation or bad capture.
      2. Any player with 75%+ actions in attacking third — very likely a zone flip.
      3. Defender (D) with 75%+ actions in attacking third — catches milder cases too.
    """
    id_filter = "AND event_id = ANY(%s)" if event_ids else ""
    params    = (event_ids,) if event_ids else ()

    checks = [
        (
            "Defender with 0% in defensive third",
            f"""SELECT player_name, side, event_id, total_def_actions,
                       pct_def_third, pct_mid_third, pct_att_third
                FROM sofascore.fact_def_maps
                WHERE position = 'D'
                  AND total_def_actions >= 5
                  AND pct_def_third = 0
                  {id_filter}
                ORDER BY event_id, player_name""",
        ),
        (
            "Any player with 75%+ actions in attacking third",
            f"""SELECT player_name, side, event_id, position, total_def_actions,
                       pct_def_third, pct_mid_third, pct_att_third
                FROM sofascore.fact_def_maps
                WHERE total_def_actions >= 5
                  AND pct_att_third >= 0.75
                  {id_filter}
                ORDER BY event_id, player_name""",
        ),
        (
            "Defender with 50%+ actions in attacking third",
            f"""SELECT player_name, side, event_id, total_def_actions,
                       pct_def_third, pct_mid_third, pct_att_third
                FROM sofascore.fact_def_maps
                WHERE position = 'D'
                  AND total_def_actions >= 5
                  AND pct_att_third >= 0.5
                  {id_filter}
                ORDER BY event_id, player_name""",
        ),
    ]

    any_flagged = False
    with conn.cursor() as cur:
        for label, sql in checks:
            cur.execute(sql, params)
            rows = cur.fetchall()
            if rows:
                any_flagged = True
                print(f"\n  [WARN] {label}:")
                for r in rows:
                    # columns vary slightly by check — unpack generically
                    name, side, eid = r[0], r[1], r[2]
                    total = r[3] if len(r) == 7 else r[4]
                    def_pct, mid_pct, att_pct = r[-3], r[-2], r[-1]
                    print(f"    {name} ({side}, event {eid}) "
                          f"total={total}  def={def_pct:.0%}  mid={mid_pct:.0%}  att={att_pct:.0%}")

    if not any_flagged:
        print("  All map data passed validation checks.")


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
        print("Loading shots...")
        load_shots(conn)
        print("Loading pass maps...")
        load_pass_maps(conn)
        print("Loading drib maps...")
        load_drib_maps(conn)
        print("Loading def maps...")
        load_def_maps(conn)
        print("\nDone.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
