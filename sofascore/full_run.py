import asyncio
import os
import sys
import time
import pandas as pd
from datetime import date, datetime


class Tee:
    """Write output to both stdout and a log file."""
    def __init__(self, filepath):
        self.terminal = sys.stdout
        self.log = open(filepath, 'a')

    def write(self, msg):
        self.terminal.write(msg)
        self.log.write(msg)

    def flush(self):
        self.terminal.flush()
        self.log.flush()


log_path = f"logs/full_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
os.makedirs("logs", exist_ok=True)
sys.stdout = Tee(log_path)
print(f"Logging to {log_path}\n")

from scraping_script import (
    build_events_df, get_match_player_stats_full, SEASONS, OUTPUT_DIR
)
from heatmap_script import fetch_heatmaps_for_dates
from shot_script import fetch_shot_tables_for_dates
from combined_map_script import fetch_all_maps_for_dates

# ── Config ────────────────────────────────────────────────────────────────────

start_date     = '2026-03-17'
end_date       = str(date.today())
overwrite      = True   # set to True to re-fetch already-scraped matches
statuses       = ('Ended', 'AET', 'AP')
adhoc_event_id = 15388438   # set to an event_id (e.g. 12345678) to scrape a single match


# ── Runner ────────────────────────────────────────────────────────────────────

async def main():
    _total_start = time.time()

    df_matches = build_events_df(SEASONS)
    print(f"Total matches loaded: {len(df_matches)}")
    print(df_matches.groupby(['season', 'status']).size(), "\n")

    if adhoc_event_id:
        subset = df_matches[df_matches['event_id'] == adhoc_event_id].reset_index(drop=True)
        if subset.empty:
            print(f"event_id {adhoc_event_id} not found — exiting")
            return
        print(f"Ad-hoc scrape: {subset.iloc[0]['home_team']} vs {subset.iloc[0]['away_team']} ({subset.iloc[0]['date']})\n")
    else:
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

    to_scrape = subset if overwrite else subset[~subset['event_id'].apply(lambda eid: os.path.exists(f"{OUTPUT_DIR}/{eid}.csv"))]
    print(f"  {len(to_scrape)} to scrape, {len(subset) - len(to_scrape)} already exist\n")

    for i, (_, row) in enumerate(to_scrape.iterrows(), 1):
        event_id = row['event_id']
        filename = f"{OUTPUT_DIR}/{event_id}.csv"
        print(f"[{i}/{len(to_scrape)}] {row['home_team']} vs {row['away_team']} ({row['date']}) — id: {event_id}")
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

    fetch_heatmaps_for_dates(subset, start_date, end_date, overwrite=overwrite)

    print(f"\nHeatmaps done in {(time.time() - t0) / 60:.1f}m\n")

    # ── 3. Shots ──────────────────────────────────────────────────────────────
    print("=" * 60)
    print("  SHOTS")
    print("=" * 60)
    t0 = time.time()

    fetch_shot_tables_for_dates(subset, start_date, end_date, overwrite=overwrite)

    print(f"\nShots done in {(time.time() - t0) / 60:.1f}m\n")

    # ── 4. Combined maps (Pass / Drib / Def) ──────────────────────────────────
    print("=" * 60)
    print("  COMBINED MAPS  (Pass / Drib / Def)")
    print("=" * 60)
    t0 = time.time()

    await fetch_all_maps_for_dates(subset, start_date, end_date, overwrite=overwrite)

    print(f"\nMaps done in {(time.time() - t0) / 60:.1f}m\n")

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed = time.time() - _total_start
    print("=" * 60)
    print(f"  Total run time: {elapsed // 60:.0f}m {elapsed % 60:.1f}s")
    print("=" * 60)


asyncio.run(main())
