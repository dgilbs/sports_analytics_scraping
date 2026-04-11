import asyncio
import time
from datetime import date
import pandas as pd
from scraping_script import build_events_df, SEASONS
from action_map_script import (
    screenshot_all_maps,
    SHOT_DIR
)

start_date = '2026-03-01'
end_date   = str(date.today())
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

    for i, row in subset.iterrows():
        match_url = row['match_url']
        event_id  = row['event_id']
        print(f"[{i+1}/{len(subset)}] {row['home_team']} vs {row['away_team']} ({row['date']})")

        for tab, output_dir in TABS:
            await screenshot_all_maps(match_url, event_id, tab=tab, output_dir=output_dir)

    elapsed = time.time() - _start
    print(f"\nTotal run time: {elapsed // 60:.0f}m {elapsed % 60:.1f}s")


asyncio.run(main())
