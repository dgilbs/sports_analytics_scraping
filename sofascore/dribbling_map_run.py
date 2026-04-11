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
