import sys
import os
import asyncio
import time
import pandas as pd
from datetime import date, datetime

from scraping_script import build_events_df, SEASONS
from action_map_script import (
    screenshot_all_maps,
    SHOT_DIR
)


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


log_path = f"logs/action_map_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
os.makedirs("logs", exist_ok=True)
sys.stdout = Tee(log_path)
print(f"Logging to {log_path}\n")


start_date = '2026-03-01'
end_date   = '2026-03-17'
statuses   = ('Ended', 'AET', 'AP')

TABS = [
    ('Shot', SHOT_DIR),
]


async def main():
    _start = time.time()

    df_matches = build_events_df(SEASONS)
    print(f"Total matches loaded: {len(df_matches)}")
    print(df_matches.groupby(['season', 'status']).size(), "\n")

    mask = (
        (df_matches['date'] >= pd.to_datetime(start_date).date()) &
        (df_matches['date'] <= pd.to_datetime(end_date).date()) &
        (df_matches['status'].isin(statuses))
    )
    subset = df_matches[mask].reset_index(drop=True)
    print(f"Scraping action maps for {len(subset)} matches ({start_date} → {end_date})\n")

    for i, (_, row) in enumerate(subset.iterrows(), 1):
        match_url = row['match_url']
        event_id  = row['event_id']
        print(f"[{i}/{len(subset)}] {row['home_team']} vs {row['away_team']} ({row['date']})")

        for tab, output_dir in TABS:
            await screenshot_all_maps(match_url, event_id, tab=tab, output_dir=output_dir)

    elapsed = time.time() - _start
    print(f"\nTotal run time: {elapsed // 60:.0f}m {elapsed % 60:.1f}s")


asyncio.run(main())
