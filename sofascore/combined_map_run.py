import asyncio
import time
from datetime import date
from scraping_script import build_events_df, SEASONS
from combined_map_script import fetch_all_maps_for_dates
from passing_map_script import load_pass_map_files
from dribbling_map_script import load_drib_map_files
from defensive_map_script import load_def_map_files

start_date = '2026-03-01'
end_date   = '2026-03-17'
overwrite  = True  # set to True to re-fetch even if file already exists


async def main():
    _start = time.time()

    df_matches = build_events_df(SEASONS)
    print(f"Total matches loaded: {len(df_matches)}")
    print(df_matches.groupby(['season', 'status']).size(), "\n")

    await fetch_all_maps_for_dates(df_matches, start_date, end_date, overwrite=overwrite)

    elapsed = time.time() - _start
    print(f"\nTotal run time: {elapsed // 60:.0f}m {elapsed % 60:.1f}s")


asyncio.run(main())
