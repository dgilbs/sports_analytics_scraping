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


log_path = f"logs/dribbling_map_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
os.makedirs("logs", exist_ok=True)
sys.stdout = Tee(log_path)
print(f"Logging to {log_path}\n")
import asyncio
import time
from datetime import date
from scraping_script import build_events_df, SEASONS
from dribbling_map_script import fetch_drib_maps_for_dates, load_drib_map_files

start_date = '2026-03-01'
end_date   = str(date.today())
overwrite  = False  # set to True to re-fetch even if file already exists


async def main():
    _start = time.time()

    df_matches = build_events_df(SEASONS)
    print(f"Total matches loaded: {len(df_matches)}")
    print(df_matches.groupby(['season', 'status']).size(), "\n")

    await fetch_drib_maps_for_dates(df_matches, start_date, end_date, overwrite=overwrite)

    df_drib = load_drib_map_files()
    print(df_drib[['player_name', 'team', 'position',
                   'dribbles_won', 'dribbles_lost', 'dribble_success',
                   'carry_segments',
                   'drib_att_third', 'carry_att_third']].to_string())

    elapsed = time.time() - _start
    print(f"\nTotal run time: {elapsed // 60:.0f}m {elapsed % 60:.1f}s")


asyncio.run(main())