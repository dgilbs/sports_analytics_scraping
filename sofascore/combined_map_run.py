import sys
import os
from datetime import datetime


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


log_path = f"logs/combined_map_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
os.makedirs("logs", exist_ok=True)
sys.stdout = Tee(log_path)
print(f"Logging to {log_path}\n")
import asyncio
import time
from datetime import date
from scraping_script import build_events_df, SEASONS
from combined_map_script import fetch_all_maps_for_dates
from passing_map_script import load_pass_map_files
from dribbling_map_script import load_drib_map_files
from defensive_map_script import load_def_map_files

start_date  = '2026-03-01'
end_date    = '2026-03-17'
overwrite   = True   # set to True to re-fetch even if file already exists
adhoc_event_id = None  # set to an event_id (e.g. 12345678) to scrape a single match


async def main():
    _start = time.time()

    df_matches = build_events_df(SEASONS)
    print(f"Total matches loaded: {len(df_matches)}")
    print(df_matches.groupby(['season', 'status']).size(), "\n")

    if adhoc_event_id:
        row = df_matches[df_matches['event_id'] == adhoc_event_id]
        if row.empty:
            print(f"event_id {adhoc_event_id} not found in match list")
        else:
            row = row.iloc[0]
            print(f"Ad-hoc scrape: {row['home_team']} vs {row['away_team']} ({row['date']})")
            from combined_map_script import scrape_all_maps
            await scrape_all_maps(row['match_url'], row['event_id'], row, overwrite=overwrite, game_num=1, total_games=1)
    else:
        await fetch_all_maps_for_dates(df_matches, start_date, end_date, overwrite=overwrite)

    elapsed = time.time() - _start
    print(f"\nTotal run time: {elapsed // 60:.0f}m {elapsed % 60:.1f}s")


asyncio.run(main())