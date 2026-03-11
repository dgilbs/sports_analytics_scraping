#!/usr/bin/env python3
"""
Fuzzy-match 2024 draft list player names to player_ids in fotmob.dim_players.

Outputs fotmob/dbt_fotmob/seeds/player_id_mapping_2024.csv with columns:
  seed_name    -- name from draft_list_2024.csv
  player_id    -- matched player_id from dim_players
  db_name      -- matched name from dim_players
  score        -- match confidence 0-100
  needs_review -- True if score < REVIEW_THRESHOLD or multiple close matches

Review the output, fix any bad matches, then run `dbt seed` to load it.

Requires env vars: NEON_HOST, NEON_USER, NEON_PASSWORD, NEON_DBNAME
"""

import os
import csv
import psycopg2
from rapidfuzz import process, fuzz

SEED_PATH    = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "dbt_fotmob", "seeds", "draft_list_2024.csv")
OUTPUT_PATH  = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "dbt_fotmob", "seeds", "player_id_mapping_2024.csv")

# Matches below this score are flagged for manual review
REVIEW_THRESHOLD = 90


def get_db_players():
    conn = psycopg2.connect(
        host=os.environ["NEON_HOST"],
        user=os.environ["NEON_USER"],
        password=os.environ["NEON_PASSWORD"],
        dbname=os.environ["NEON_DBNAME"],
        port=int(os.environ.get("NEON_PORT", 5432)),
        sslmode="require",
    )
    with conn.cursor() as cur:
        cur.execute("SELECT player_id, player_name FROM fotmob.dim_players ORDER BY player_name")
        rows = cur.fetchall()
    conn.close()
    # Returns {player_name: player_id}
    return {name: pid for pid, name in rows}


def normalize(name):
    """Lowercase and strip accents for more forgiving comparison."""
    import unicodedata
    name = name.lower().strip()
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    return name


def match_players(seed_names, db_players):
    db_names      = list(db_players.keys())
    db_names_norm = [normalize(n) for n in db_names]

    results = []
    for seed_name in seed_names:
        seed_norm = normalize(seed_name)

        # Try exact normalized match first
        exact_idx = next((i for i, n in enumerate(db_names_norm) if n == seed_norm), None)
        if exact_idx is not None:
            db_name   = db_names[exact_idx]
            results.append({
                "seed_name":    seed_name,
                "player_id":    db_players[db_name],
                "db_name":      db_name,
                "score":        100,
                "needs_review": False,
            })
            continue

        # Fuzzy match against normalized names
        matches = process.extract(
            seed_norm,
            db_names_norm,
            scorer=fuzz.token_sort_ratio,
            limit=3,
        )

        best_name_norm, best_score, best_idx = matches[0]
        db_name = db_names[best_idx]

        # Flag if score is low OR if second match is very close (ambiguous)
        needs_review = best_score < REVIEW_THRESHOLD
        if len(matches) > 1 and (matches[0][1] - matches[1][1]) < 5:
            needs_review = True

        results.append({
            "seed_name":    seed_name,
            "player_id":    db_players[db_name],
            "db_name":      db_name,
            "score":        best_score,
            "needs_review": needs_review,
        })

    return results


def main():
    print("Loading seed file...")
    with open(SEED_PATH, newline="", encoding="utf-8") as f:
        seed_names = [row["player"] for row in csv.DictReader(f) if row.get("player")]

    print(f"  {len(seed_names)} players in seed file")

    print("Loading players from database...")
    db_players = get_db_players()
    print(f"  {len(db_players)} players in dim_players")

    print("Matching names...")
    results = match_players(seed_names, db_players)

    n_review = sum(1 for r in results if r["needs_review"])
    n_matched = sum(1 for r in results if not r["needs_review"])

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["seed_name", "player_id", "db_name", "score", "needs_review"])
        writer.writeheader()
        writer.writerows(sorted(results, key=lambda r: (not r["needs_review"], r["seed_name"])))

    print(f"\nResults written to {OUTPUT_PATH}")
    print(f"  Auto-matched:  {n_matched}")
    print(f"  Needs review:  {n_review}")
    print(f"\nReview the '{OUTPUT_PATH}' file, fix any bad matches,")
    print("then run `dbt seed` to load it into Postgres.")


if __name__ == "__main__":
    main()
