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


log_path = f"logs/code_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
os.makedirs("logs", exist_ok=True)
sys.stdout = Tee(log_path)
print(f"Logging to {log_path}\n")
import os
import time
import pandas as pd
from datetime import date
from scraping_script import (
    build_events_df, get_match_player_stats_full, load_all_match_files,
    SEASONS, OUTPUT_DIR
)

# ── Fetch stats — one file per match ─────────────────────────────────────────

_start = time.time()

# Build match list
df_matches = build_events_df(SEASONS)
print(f"Total matches loaded: {len(df_matches)}")
print(df_matches.groupby(['season', 'status']).size(), "\n")

start_date = '2026-03-01'
end_date   = '2026-03-17'
statuses   = ('Ended', 'AET', 'AP')
overwrite  = True  # set to True to re-fetch even if file already exists

mask = (
    (df_matches['date'] >= pd.to_datetime(start_date).date()) &
    (df_matches['date'] <= pd.to_datetime(end_date).date()) &
    (df_matches['status'].isin(statuses))
)
subset = df_matches[mask].reset_index(drop=True)
print(f"Fetching stats for {len(subset)} matches ({start_date} → {end_date})\n")

for i, row in subset.iterrows():
    event_id   = row['event_id']
    match_date = row['date']
    filename   = f"{OUTPUT_DIR}/{event_id}.csv"

    if os.path.exists(filename) and not overwrite:
        print(f"[{i+1}/{len(subset)}] Skipping {event_id} — already exists")
        continue

    print(f"[{i+1}/{len(subset)}] {row['home_team']} vs {row['away_team']} ({match_date})")
    rows = get_match_player_stats_full(row)

    if rows:
        pd.DataFrame(rows).to_csv(filename, index=False)
        print(f"  {'Overwrote' if overwrite else 'Saved'} {len(rows)} rows → {filename}")
    else:
        print(f"  No data returned, skipping")

print(f"\nDone — files in ./{OUTPUT_DIR}/")

# Load all files into one dataframe
df_all = load_all_match_files()

elapsed = time.time() - _start
print(f"\nTotal run time: {elapsed // 60:.0f}m {elapsed % 60:.1f}s")