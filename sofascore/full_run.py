import asyncio
import os
import time
import pandas as pd
from datetime import date

from scraping_script import (
    build_events_df, get_match_player_stats_full, SEASONS, OUTPUT_DIR
)
from heatmap_script import fetch_heatmaps_for_dates
from shot_script import fetch_shot_tables_for_dates
from combined_map_script import fetch_all_maps_for_dates

# ── Config ────────────────────────────────────────────────────────────────────

start_date = '2026-03-01'
end_date   = str(date.today())
overwrite  = False   # set to True to re-fetch already-scraped matches
statuses   = ('Ended', 'AET', 'AP')


# ── Runner ────────────────────────────────────────────────────────────────────

async def main():
    _total_start = time.time()

    df_matches = build_events_df(SEASONS)
    print(f"Total matches loaded: {len(df_matches)}")
    print(df_matches.groupby(['season', 'status']).size(), "\n")

    mask = (
        (df_matches['date'] >= pd.to_datetime(start_date).date()) &
        (df_matches['date'] <= pd.to_datetime(end_date).date()) &
        (df_matches['status'].isin(statuses))
    )
    subset = df_matches[mask].reset_index(drop=True)
    print(f"Processing {len(subset)} matches ({start_date} → {end_date})\n")

    # ── 1. Player stats ───────────────────────────────────────────────────────
    print("=" * 60)
    print("  PLAYER STATS")
    print("=" * 60)
    t0 = time.time()

    for i, row in subset.iterrows():
        event_id = row['event_id']
        filename = f"{OUTPUT_DIR}/{event_id}.csv"
        if os.path.exists(filename) and not overwrite:
            print(f"[{i+1}/{len(subset)}] Skipping {event_id} — already exists")
            continue
        print(f"[{i+1}/{len(subset)}] {row['home_team']} vs {row['away_team']} ({row['date']})")
        rows = get_match_player_stats_full(row)
        if rows:
            pd.DataFrame(rows).to_csv(filename, index=False)
            print(f"  Saved {len(rows)} rows → {filename}")
        else:
            print(f"  No data returned")

    print(f"\nPlayer stats done in {(time.time() - t0) / 60:.1f}m\n")

    # ── 2. Heatmaps ───────────────────────────────────────────────────────────
    print("=" * 60)
    print("  HEATMAPS")
    print("=" * 60)
    t0 = time.time()

    fetch_heatmaps_for_dates(df_matches, start_date, end_date, overwrite=overwrite)

    print(f"\nHeatmaps done in {(time.time() - t0) / 60:.1f}m\n")

    # ── 3. Shots ──────────────────────────────────────────────────────────────
    print("=" * 60)
    print("  SHOTS")
    print("=" * 60)
    t0 = time.time()

    fetch_shot_tables_for_dates(df_matches, start_date, end_date, overwrite=overwrite)

    print(f"\nShots done in {(time.time() - t0) / 60:.1f}m\n")

    # ── 4. Combined maps (Pass / Drib / Def) ──────────────────────────────────
    print("=" * 60)
    print("  COMBINED MAPS  (Pass / Drib / Def)")
    print("=" * 60)
    t0 = time.time()

    await fetch_all_maps_for_dates(df_matches, start_date, end_date, overwrite=overwrite)

    print(f"\nMaps done in {(time.time() - t0) / 60:.1f}m\n")

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed = time.time() - _total_start
    print("=" * 60)
    print(f"  Total run time: {elapsed // 60:.0f}m {elapsed % 60:.1f}s")
    print("=" * 60)


asyncio.run(main())
