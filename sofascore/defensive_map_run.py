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


log_path = f"logs/defensive_map_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
os.makedirs("logs", exist_ok=True)
sys.stdout = Tee(log_path)
print(f"Logging to {log_path}\n")
import asyncio
import time
from datetime import date
from scraping_script import build_events_df, SEASONS
from defensive_map_script import fetch_def_maps_for_dates, load_def_map_files

start_date = '2026-03-01'
end_date   = str(date.today())
overwrite  = False  # set to True to re-fetch even if file already exists


async def main():
    _start = time.time()

    df_matches = build_events_df(SEASONS)
    print(f"Total matches loaded: {len(df_matches)}")
    print(df_matches.groupby(['season', 'status']).size(), "\n")

    await fetch_def_maps_for_dates(df_matches, start_date, end_date, overwrite=overwrite)

    df_def = load_def_map_files()
    print(df_def[['player_name', 'team', 'position',
                  'tackle_won', 'missed_tackle', 'interception',
                  'clearance', 'block', 'recovery',
                  'pct_def_third', 'pct_mid_third', 'pct_att_third']].to_string())

    elapsed = time.time() - _start
    print(f"\nTotal run time: {elapsed // 60:.0f}m {elapsed % 60:.1f}s")


asyncio.run(main())