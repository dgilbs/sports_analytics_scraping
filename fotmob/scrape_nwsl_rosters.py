#!/usr/bin/env python3
"""
Scrape current NWSL team rosters from FBRef and output to a CSV.

Usage:
    python fotmob/scrape_nwsl_rosters.py

Output:
    fotmob/data/nwsl_rosters_2026.csv

Columns:
    team, player, player_id, nationality, position, age, shirt_number
"""

import time
import requests
import pandas as pd
from io import StringIO

SEASON      = 2026
LEAGUE_ID   = 182
LEAGUE_TAG  = "NWSL-Stats"
OUTPUT_PATH = "fotmob/data/nwsl_rosters_2026.csv"
DELAY       = 10  # seconds between requests (be polite to FBRef)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    )
}


def get_squad_links():
    """Scrape the NWSL standings page and return list of (team_name, squad_link, squad_id)."""
    url = f"https://fbref.com/en/comps/{LEAGUE_ID}/{SEASON}/{SEASON}-{LEAGUE_TAG}"
    print(f"Fetching standings: {url}")
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    # First table on the page is the standings
    df = pd.read_html(StringIO(response.text), extract_links="body")[0]
    df.columns = [c[0].lower() if isinstance(c, tuple) else c.lower() for c in df.columns]

    squads = []
    for _, row in df.iterrows():
        squad_cell = row.get("squad", None)
        if not isinstance(squad_cell, tuple):
            continue
        team_name, squad_link = squad_cell
        if not squad_link or "/squads/" not in squad_link:
            continue
        squad_id = squad_link.split("/squads/")[1].split("/")[0]
        squads.append({
            "team":       team_name,
            "squad_id":   squad_id,
            "squad_link": squad_link,
        })

    print(f"  Found {len(squads)} teams")
    return squads


def scrape_roster(squad):
    """Scrape a single team's roster page and return a DataFrame."""
    url = "https://fbref.com" + squad["squad_link"]
    table_id = f"stats_standard_{LEAGUE_ID}"
    print(f"  Scraping {squad['team']} ... {url}")

    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    tables = pd.read_html(StringIO(response.text), extract_links="body", attrs={"id": table_id})
    if not tables:
        print(f"    Warning: no roster table found for {squad['team']}")
        return pd.DataFrame()

    df = tables[0]

    # Flatten multi-level columns
    df.columns = [
        (c[0][0] if isinstance(c[0], tuple) else c[0]).lower().replace(" ", "_")
        if isinstance(c, tuple) else c.lower()
        for c in df.columns
    ]

    # Extract player name and player_id from link tuple
    player_col = next((c for c in df.columns if "player" in c), None)
    if player_col and df[player_col].dtype == object:
        df["player_id"] = df[player_col].apply(
            lambda x: x[1].split("/")[3] if isinstance(x, tuple) and x[1] else None
        )
        df["player"] = df[player_col].apply(
            lambda x: x[0] if isinstance(x, tuple) else x
        )

    # Extract nationality (drop link)
    for col in ["nation", "nationality"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x[0] if isinstance(x, tuple) else x)

    # Select and rename relevant columns
    col_map = {
        "player":   "player",
        "player_id":"player_id",
        "nation":   "nationality",
        "pos":      "position",
        "age":      "age",
        "#":        "shirt_number",
    }
    keep = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=keep)[list(keep.values())].copy()

    # Drop summary/header rows
    df = df[df["player"].notna() & (df["player"] != "Player")]

    df["team"] = squad["team"]
    return df


def main():
    squads = get_squad_links()
    if not squads:
        print("No squads found — the page structure may have changed.")
        return

    all_rosters = []
    for i, squad in enumerate(squads):
        time.sleep(DELAY)
        try:
            df = scrape_roster(squad)
            if not df.empty:
                all_rosters.append(df)
                print(f"    {len(df)} players")
        except Exception as e:
            print(f"    Error scraping {squad['team']}: {e}")

    if not all_rosters:
        print("No roster data collected.")
        return

    combined = pd.concat(all_rosters, ignore_index=True)

    # Reorder columns
    cols = ["team", "player", "player_id", "nationality", "position", "age", "shirt_number"]
    cols = [c for c in cols if c in combined.columns]
    combined = combined[cols]

    combined.to_csv(OUTPUT_PATH, index=False)
    print(f"\nSaved {len(combined)} players across {len(all_rosters)} teams to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
