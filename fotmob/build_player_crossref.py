#!/usr/bin/env python3
"""
Fuzzy-match FBRef NWSL player IDs to FotMob player IDs.

Outputs fotmob/dbt_fotmob/seeds/fbref_fotmob_crossref.csv with columns:
  fbref_player_id   -- fbref.dim_players.id
  fbref_name        -- fbref.dim_players.player
  fotmob_player_id  -- fotmob.dim_players.player_id
  fotmob_name       -- fotmob.dim_players.player_name
  score             -- match confidence 0-100
  needs_review      -- True if score < REVIEW_THRESHOLD or ambiguous

Review the output and fix any bad matches, then run `dbt seed`.
The crossref enables joining fbref.f_player_match_misc (cards, own goals,
penalties) and fbref.f_player_match_keeper into fotmob fantasy models.

Requires env vars: NEON_HOST, NEON_USER, NEON_PASSWORD, NEON_DBNAME
"""

import os
import csv
import psycopg2
from rapidfuzz import process, fuzz

OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "dbt_fotmob", "seeds", "fbref_fotmob_crossref.csv",
)

REVIEW_THRESHOLD = 90


def get_conn():
    return psycopg2.connect(
        host=os.environ["NEON_HOST"],
        user=os.environ["NEON_USER"],
        password=os.environ["NEON_PASSWORD"],
        dbname=os.environ["NEON_DBNAME"],
        port=int(os.environ.get("NEON_PORT", 5432)),
        sslmode="require",
    )


def get_fbref_nwsl_players(conn):
    """Return {fbref_id: player_name} for players who appeared in NWSL matches."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT p.id, p.player
            FROM fbref.dim_players p
            JOIN fbref.f_player_match_summary s ON p.id = s.player_id
            JOIN fbref.dim_matches m ON s.match_id = m.id
            WHERE m.competition_id = 182  -- NWSL
            ORDER BY p.player
        """)
        return {row[0]: row[1] for row in cur.fetchall()}


def get_fotmob_players(conn):
    """Return {fotmob_player_id: player_name}."""
    with conn.cursor() as cur:
        cur.execute("SELECT player_id, player_name FROM fotmob.dim_players ORDER BY player_name")
        return {row[0]: row[1] for row in cur.fetchall()}


def normalize(name):
    import unicodedata
    name = name.lower().strip()
    name = unicodedata.normalize("NFD", name)
    return "".join(c for c in name if unicodedata.category(c) != "Mn")


def build_crossref(fbref_players, fotmob_players):
    fotmob_ids    = list(fotmob_players.keys())
    fotmob_names  = list(fotmob_players.values())
    fotmob_norms  = [normalize(n) for n in fotmob_names]

    results = []
    for fbref_id, fbref_name in fbref_players.items():
        fbref_norm = normalize(fbref_name)

        # Exact normalized match
        exact_idx = next((i for i, n in enumerate(fotmob_norms) if n == fbref_norm), None)
        if exact_idx is not None:
            results.append({
                "fbref_player_id":  fbref_id,
                "fbref_name":       fbref_name,
                "fotmob_player_id": fotmob_ids[exact_idx],
                "fotmob_name":      fotmob_names[exact_idx],
                "score":            100,
                "needs_review":     False,
            })
            continue

        # Fuzzy match
        matches = process.extract(
            fbref_norm,
            fotmob_norms,
            scorer=fuzz.token_sort_ratio,
            limit=3,
        )
        best_norm, best_score, best_idx = matches[0]
        ambiguous = len(matches) > 1 and (matches[0][1] - matches[1][1]) < 5

        results.append({
            "fbref_player_id":  fbref_id,
            "fbref_name":       fbref_name,
            "fotmob_player_id": fotmob_ids[best_idx],
            "fotmob_name":      fotmob_names[best_idx],
            "score":            best_score,
            "needs_review":     best_score < REVIEW_THRESHOLD or ambiguous,
        })

    return results


def main():
    print("Connecting to database...")
    conn = get_conn()

    print("Loading FBRef NWSL players...")
    fbref_players = get_fbref_nwsl_players(conn)
    print(f"  {len(fbref_players)} NWSL players in fbref.dim_players")

    print("Loading FotMob players...")
    fotmob_players = get_fotmob_players(conn)
    print(f"  {len(fotmob_players)} players in fotmob.dim_players")

    conn.close()

    print("Matching names...")
    results = build_crossref(fbref_players, fotmob_players)

    n_review  = sum(1 for r in results if r["needs_review"])
    n_matched = sum(1 for r in results if not r["needs_review"])

    # Sort: needs_review first, then alphabetical by fbref_name
    results.sort(key=lambda r: (not r["needs_review"], r["fbref_name"]))

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "fbref_player_id", "fbref_name",
            "fotmob_player_id", "fotmob_name",
            "score", "needs_review",
        ])
        writer.writeheader()
        writer.writerows(results)

    print(f"\nWritten to {OUTPUT_PATH}")
    print(f"  Auto-matched:  {n_matched}")
    print(f"  Needs review:  {n_review}")
    print(f"\nReview flagged rows, then run `dbt seed` to load.")


if __name__ == "__main__":
    main()
