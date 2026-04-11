import asyncio
import time
from datetime import date
from scraping_script import build_events_df, SEASONS
from passing_map_script import fetch_pass_maps_for_dates, load_pass_map_files

start_date = '2026-03-01'
end_date   = str(date.today())
overwrite  = False  # set to True to re-fetch even if file already exists


async def main():
    _start = time.time()

    df_matches = build_events_df(SEASONS)
    print(f"Total matches loaded: {len(df_matches)}")
    print(df_matches.groupby(['season', 'status']).size(), "\n")

    await fetch_pass_maps_for_dates(df_matches, start_date, end_date, overwrite=overwrite)

    df_passes = load_pass_map_files()
    print(df_passes[['player_name', 'team', 'position',
                      'passes_total', 'passes_accurate', 'pass_accuracy',
                      'pct_forward', 'progressive_passes',
                      'dest_att_third', 'dest_left_wing', 'dest_right_wing']].to_string())

    elapsed = time.time() - _start
    print(f"\nTotal run time: {elapsed // 60:.0f}m {elapsed % 60:.1f}s")


asyncio.run(main())
