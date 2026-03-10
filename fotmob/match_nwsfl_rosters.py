#!/usr/bin/env python3
"""
Match nwsfl_rosters_2026.csv players to player IDs and most recent team.

Steps:
1. Exact match against player_id_mapping.csv seed_name
2. Fuzzy match against fotmob.dim_players for any unmatched names
3. Keep only score >= 80
4. Join most recent team from fotmob.fantasy_match_points

Output: fotmob/dbt_fotmob/seeds/nwsfl_roster_matched.csv
"""

import os
import csv
import unicodedata
import psycopg2
import pandas as pd
from rapidfuzz import process, fuzz
from dotenv import load_dotenv

load_dotenv()

ROSTER_PATH  = os.path.join(os.path.dirname(__file__), "dbt_fotmob", "seeds", "nwsfl_rosters_2026.csv")
MAPPING_PATH = os.path.join(os.path.dirname(__file__), "dbt_fotmob", "seeds", "player_id_mapping.csv")
OUTPUT_PATH  = os.path.join(os.path.dirname(__file__), "dbt_fotmob", "seeds", "nwsfl_roster_matched.csv")

MATCH_THRESHOLD = 80


def get_conn():
    return psycopg2.connect(
        host=os.environ["NEON_HOST"],
        user=os.environ["NEON_USER"],
        password=os.environ["NEON_PASSWORD"],
        dbname=os.environ["NEON_DBNAME"],
        port=int(os.environ.get("NEON_PORT", 5432)),
        sslmode="require",
    )


def normalize(name):
    name = name.lower().strip()
    name = unicodedata.normalize("NFD", name)
    return "".join(c for c in name if unicodedata.category(c) != "Mn")


def main():
    # ── Load roster ──────────────────────────────────────────────────────────
    roster = []
    with open(ROSTER_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            roster.append({"manager": row["Manager"], "player": row["Player"]})
    print(f"Roster: {len(roster)} players")

    # ── Load existing mapping ─────────────────────────────────────────────────
    mapping = {}  # seed_name (normalized) → {player_id, db_name, score}
    with open(MAPPING_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            score = float(row["score"])
            if score >= MATCH_THRESHOLD:
                mapping[normalize(row["seed_name"])] = {
                    "player_id": int(row["player_id"]),
                    "db_name":   row["db_name"],
                    "score":     score,
                }

    # ── Load dim_players for fuzzy fallback ────────────────────────────────────
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT player_id, player_name FROM fotmob.dim_players ORDER BY player_name")
        db_rows = cur.fetchall()
    db_ids   = [r[0] for r in db_rows]
    db_names = [r[1] for r in db_rows]
    db_norms = [normalize(n) for n in db_names]

    # ── Most recent team per player ───────────────────────────────────────────
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (p.player_id)
                p.player_id,
                fmp.team_name
            FROM fotmob.dim_players p
            JOIN fotmob.fantasy_match_points fmp ON p.player_name = fmp.player_name
            WHERE fmp.team_name IS NOT NULL
            ORDER BY p.player_id, fmp.match_date DESC
        """)
        team_rows = cur.fetchall()
    conn.close()
    recent_team = {r[0]: r[1] for r in team_rows}

    # ── Match each roster player ──────────────────────────────────────────────
    results = []
    unmatched = []

    for entry in roster:
        name  = entry["player"]
        norm  = normalize(name)

        # 1. Exact match against existing mapping
        if norm in mapping:
            m = mapping[norm]
            results.append({
                "manager":     entry["manager"],
                "player":      name,
                "player_id":   m["player_id"],
                "db_name":     m["db_name"],
                "match_score": round(m["score"], 1),
                "team":        recent_team.get(m["player_id"], ""),
            })
            continue

        # 2. Fuzzy match against dim_players
        matches = process.extract(norm, db_norms, scorer=fuzz.token_sort_ratio, limit=3)
        best_norm, best_score, best_idx = matches[0]

        if best_score >= MATCH_THRESHOLD:
            pid = db_ids[best_idx]
            results.append({
                "manager":     entry["manager"],
                "player":      name,
                "player_id":   pid,
                "db_name":     db_names[best_idx],
                "match_score": round(best_score, 1),
                "team":        recent_team.get(pid, ""),
            })
        else:
            unmatched.append({"manager": entry["manager"], "player": name, "best_score": round(best_score, 1)})
            results.append({
                "manager":     entry["manager"],
                "player":      name,
                "player_id":   "",
                "db_name":     "",
                "match_score": "",
                "team":        "",
            })

    # ── Write output ──────────────────────────────────────────────────────────
    results.sort(key=lambda r: (r["manager"], r["player"]))
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["manager", "player", "player_id", "db_name", "match_score", "team"])
        writer.writeheader()
        writer.writerows(results)

    print(f"\nMatched:   {len(results)}")
    print(f"Unmatched: {len(unmatched)}")
    if unmatched:
        print("\nPlayers below 80% threshold (not included):")
        for u in sorted(unmatched, key=lambda x: x["player"]):
            print(f"  [{u['manager']}] {u['player']}  (best score: {u['best_score']})")
    print(f"\nOutput: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
