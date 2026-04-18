#!/usr/bin/env python3
"""
Build sofascore.sofascore_fotmob_crossref by fuzzy-matching player names
within matched games across Sofascore and Fotmob.

Strategy
--------
1. Find all matches linked between the two systems (date + home team name).
2. For each linked match, pull the player lists from both sides.
3. Within each match, fuzzy-match every Sofascore player against every Fotmob
   player using rapidfuzz token_sort_ratio.
4. Keep the best Fotmob candidate per Sofascore player if score >= MIN_SCORE.
5. Aggregate across all matches: for each (ss_player_id, fm_player_id) pair,
   record match_count and avg_score.
6. Keep only the top Fotmob candidate per Sofascore player.
7. Write to sofascore.sofascore_fotmob_crossref (create/replace).

Outputs
-------
  sofascore.sofascore_fotmob_crossref
    sofascore_player_id  int
    sofascore_name       text
    fotmob_player_id     int
    fotmob_name          text
    match_count          int       (how many matches confirmed this pair)
    avg_score            float     (average rapidfuzz score 0–100)
    needs_review         bool      (avg_score < REVIEW_THRESHOLD)

Usage
-----
  export DATABASE_URL="postgresql://..."   # or rely on NEON_* env vars
  python sofascore/build_fotmob_crossref.py
"""

import os
import sys
from collections import defaultdict

import psycopg2
import psycopg2.extras
import pandas as pd
from rapidfuzz import fuzz, process

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MIN_SCORE       = 70    # minimum fuzz score to consider a candidate
REVIEW_THRESHOLD = 95   # avg_score below this → needs_review = True

# ---------------------------------------------------------------------------
# DB connection (reuse load_to_db.py pattern)
# ---------------------------------------------------------------------------
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

def get_conn():
    url = os.environ.get("DATABASE_URL")
    if url:
        return psycopg2.connect(url)
    return psycopg2.connect(
        host=os.environ["NEON_HOST"],
        user=os.environ["NEON_USER"],
        password=os.environ["NEON_PASSWORD"],
        dbname=os.environ["NEON_DBNAME"],
        sslmode="require",
    )

# ---------------------------------------------------------------------------
# Step 1: pull linked matches
# ---------------------------------------------------------------------------
LINKED_MATCHES_SQL = """
SELECT DISTINCT
    ss.event_id     AS sofascore_event_id,
    fm.match_id     AS fotmob_match_id
FROM (
    SELECT DISTINCT event_id, match_date, lower(home_team) AS home_lower
    FROM sofascore.player_match_stats
) ss
JOIN (
    SELECT DISTINCT match_id, match_date, lower(team_name) AS home_lower
    FROM fotmob.player_match_stats
    WHERE side = 'home'
) fm USING (match_date, home_lower)
"""

# ---------------------------------------------------------------------------
# Step 2: pull player lists per match
# ---------------------------------------------------------------------------
SS_PLAYERS_SQL = """
SELECT DISTINCT player_id, player_name
FROM sofascore.player_match_stats
WHERE event_id = %(event_id)s
"""

FM_PLAYERS_SQL = """
SELECT DISTINCT player_id, player_name
FROM fotmob.player_match_stats
WHERE match_id = %(match_id)s
"""

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # ---- linked matches ----
    cur.execute(LINKED_MATCHES_SQL)
    linked = cur.fetchall()
    print(f"Linked matches: {len(linked)}")

    # ---- load rejected pairs as a blocklist ----
    cur.execute("SELECT sofascore_player_id, fotmob_player_id FROM sofascore.sofascore_fotmob_crossref_rejected")
    rejected = set((r["sofascore_player_id"], r["fotmob_player_id"]) for r in cur.fetchall())
    print(f"Rejected pairs (blocklist): {len(rejected)}")

    # ---- accumulate candidate evidence ----
    # key: (ss_player_id, fm_player_id)
    # value: {ss_name, fm_name, scores: [float], count: int}
    evidence: dict[tuple, dict] = defaultdict(lambda: {"scores": [], "ss_name": "", "fm_name": ""})

    for row in linked:
        ss_event_id = row["sofascore_event_id"]
        fm_match_id = row["fotmob_match_id"]

        cur.execute(SS_PLAYERS_SQL, {"event_id": ss_event_id})
        ss_players = cur.fetchall()

        cur.execute(FM_PLAYERS_SQL, {"match_id": fm_match_id})
        fm_players = cur.fetchall()

        if not ss_players or not fm_players:
            continue

        fm_names = [p["player_name"] for p in fm_players]
        fm_by_name = {p["player_name"]: p["player_id"] for p in fm_players}

        for ss_row in ss_players:
            ss_id   = ss_row["player_id"]
            ss_name = ss_row["player_name"]

            # best Fotmob match for this Sofascore player in this game
            best = process.extractOne(
                ss_name,
                fm_names,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=MIN_SCORE,
            )
            if best is None:
                continue

            fm_name, score, _ = best
            fm_id = fm_by_name[fm_name]

            if (ss_id, fm_id) in rejected:
                continue

            key = (ss_id, fm_id)
            evidence[key]["ss_name"] = ss_name
            evidence[key]["fm_name"] = fm_name
            evidence[key]["scores"].append(score)

    print(f"Candidate pairs found: {len(evidence)}")

    # ---- aggregate ----
    rows = []
    for (ss_id, fm_id), v in evidence.items():
        avg_score   = sum(v["scores"]) / len(v["scores"])
        match_count = len(v["scores"])
        rows.append({
            "sofascore_player_id": ss_id,
            "sofascore_name":      v["ss_name"],
            "fotmob_player_id":    fm_id,
            "fotmob_name":         v["fm_name"],
            "match_count":         match_count,
            "avg_score":           round(avg_score, 2),
            "needs_review":        avg_score < REVIEW_THRESHOLD,
        })

    df = pd.DataFrame(rows)

    # ---- keep best Fotmob match per Sofascore player ----
    # Sort so highest avg_score + most matches wins
    df = (
        df.sort_values(["sofascore_player_id", "avg_score", "match_count"],
                       ascending=[True, False, False])
          .drop_duplicates(subset="sofascore_player_id", keep="first")
          .reset_index(drop=True)
    )

    print(f"Final crossref rows: {len(df)}")
    print("\nSample (needs_review=True):")
    print(df[df["needs_review"]].head(10).to_string(index=False))
    print("\nSample (needs_review=False):")
    print(df[~df["needs_review"]].head(10).to_string(index=False))

    # ---- write to DB ----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sofascore.sofascore_fotmob_crossref (
            sofascore_player_id  int          NOT NULL,
            sofascore_name       text         NOT NULL,
            fotmob_player_id     int          NOT NULL,
            fotmob_name          text         NOT NULL,
            match_count          int          NOT NULL,
            avg_score            numeric(6,2) NOT NULL,
            needs_review         boolean      NOT NULL,
            PRIMARY KEY (sofascore_player_id)
        )
    """)

    # Upsert — replace any existing rows
    upsert_sql = """
        INSERT INTO sofascore.sofascore_fotmob_crossref
            (sofascore_player_id, sofascore_name, fotmob_player_id, fotmob_name,
             match_count, avg_score, needs_review)
        VALUES %s
        ON CONFLICT (sofascore_player_id) DO UPDATE SET
            sofascore_name    = EXCLUDED.sofascore_name,
            fotmob_player_id  = EXCLUDED.fotmob_player_id,
            fotmob_name       = EXCLUDED.fotmob_name,
            match_count       = EXCLUDED.match_count,
            avg_score         = EXCLUDED.avg_score,
            needs_review      = EXCLUDED.needs_review
    """

    records = [
        (
            r.sofascore_player_id, r.sofascore_name,
            r.fotmob_player_id,    r.fotmob_name,
            r.match_count,         r.avg_score,
            r.needs_review,
        )
        for r in df.itertuples(index=False)
    ]

    psycopg2.extras.execute_values(cur, upsert_sql, records)
    conn.commit()
    print(f"\nWrote {len(records)} rows to sofascore.sofascore_fotmob_crossref")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
