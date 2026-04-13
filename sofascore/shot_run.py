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


log_path = f"logs/shot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
os.makedirs("logs", exist_ok=True)
sys.stdout = Tee(log_path)
print(f"Logging to {log_path}\n")
import time
from datetime import date
from scraping_script import build_events_df, SEASONS
from shot_script import fetch_shot_tables_for_dates, load_all_shot_tables

_start = time.time()

start_date = '2026-03-01'
end_date   = str(date.today())
overwrite  = False  # set to True to re-fetch even if file already exists

df_matches = build_events_df(SEASONS)
print(f"Total matches loaded: {len(df_matches)}")
print(df_matches.groupby(['season', 'status']).size(), "\n")

fetch_shot_tables_for_dates(df_matches, start_date, end_date, overwrite=overwrite)

df_shots = load_all_shot_tables()

# Quick summary — shots by player across all loaded matches
summary = (
    df_shots.groupby(['player_name', 'team'])
    .agg(
        shots       = ('shot_type', 'count'),
        goals       = ('is_goal', 'sum'),
        on_target   = ('is_on_target', 'sum'),
        blocked     = ('is_blocked', 'sum'),
        open_play   = ('situation', lambda x: (x == 'open-play').sum()),
        right_foot  = ('body_part', lambda x: (x == 'right-foot').sum()),
        left_foot   = ('body_part', lambda x: (x == 'left-foot').sum()),
        headed      = ('body_part', lambda x: (x == 'head').sum()),
    )
    .sort_values('shots', ascending=False)
    .reset_index()
)
summary['conversion']    = (summary['goals'] / summary['shots']).round(3)
summary['on_target_pct'] = (summary['on_target'] / summary['shots']).round(3)

print(summary.head(20).to_string())

elapsed = time.time() - _start
print(f"\nTotal run time: {elapsed // 60:.0f}m {elapsed % 60:.1f}s")