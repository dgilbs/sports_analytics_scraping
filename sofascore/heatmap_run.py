import time
from datetime import date
from heatmap_script import (
    build_events_df, fetch_heatmaps_for_dates, load_all_heatmap_files,
    SEASONS
)

_start = time.time()

df_matches = build_events_df(SEASONS)
print(f"Total matches loaded: {len(df_matches)}")
print(df_matches.groupby(['season', 'status']).size(), "\n")

start_date = '2026-03-01'
end_date   = str(date.today())
overwrite  = False  # set to True to re-fetch even if file already exists

fetch_heatmaps_for_dates(df_matches, start_date, end_date, overwrite=overwrite)

df_heatmaps = load_all_heatmap_files()
print(df_heatmaps[['player_name', 'team', 'position', 'touch_count',
                    'defensive_third', 'middle_third', 'attacking_third',
                    'left_wing', 'central', 'right_wing']].head(20).to_string())

elapsed = time.time() - _start
print(f"\nTotal run time: {elapsed // 60:.0f}m {elapsed % 60:.1f}s")
