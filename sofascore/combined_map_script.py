import os
import asyncio
import requests
import pandas as pd
from playwright.async_api import async_playwright

from passing_map_script import (
    parse_pass_svg, summarize_pass_actions,
    PASS_SVG_DIR, PASS_DATA_DIR,
)
from dribbling_map_script import (
    parse_drib_svg, summarize_drib_actions,
    DRIB_SVG_DIR, DRIB_DATA_DIR,
)
from defensive_map_script import (
    parse_def_svg, summarize_def_actions,
    DEF_SVG_DIR, DEF_DATA_DIR,
)

# ── Config ────────────────────────────────────────────────────────────────────

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://www.sofascore.com/"
}

TAB_CONFIG = [
    {
        'tab':          'Pass',
        'svg_dir':      PASS_SVG_DIR,
        'data_dir':     PASS_DATA_DIR,
        'parse_fn':     parse_pass_svg,
        'summarize_fn': summarize_pass_actions,
    },
    {
        'tab':          'Drib',
        'svg_dir':      DRIB_SVG_DIR,
        'data_dir':     DRIB_DATA_DIR,
        'parse_fn':     parse_drib_svg,
        'summarize_fn': summarize_drib_actions,
    },
    {
        'tab':          'Def',
        'svg_dir':      DEF_SVG_DIR,
        'data_dir':     DEF_DATA_DIR,
        'parse_fn':     parse_def_svg,
        'summarize_fn': summarize_def_actions,
    },
]


# ── Core scraper ──────────────────────────────────────────────────────────────

async def scrape_all_maps(match_url, event_id, row, overwrite=False):
    filenames = {cfg['tab']: f"{cfg['data_dir']}/{event_id}.csv" for cfg in TAB_CONFIG}

    tabs_to_scrape = [
        cfg for cfg in TAB_CONFIG
        if overwrite or not os.path.exists(filenames[cfg['tab']])
    ]
    if not tabs_to_scrape:
        print(f"  Skipping {event_id} — all tabs already exist")
        return

    resp = requests.get(
        f"https://api.sofascore.com/api/v1/event/{event_id}/lineups",
        headers=headers
    )
    if resp.status_code != 200:
        print(f"  No lineups for {event_id}")
        return

    lineups = resp.json()
    player_meta = {}
    for side, team_name in [('home', row['home_team']), ('away', row['away_team'])]:
        for entry in lineups.get(side, {}).get('players', []):
            p = entry['player']
            player_meta[str(p['id'])] = {
                'player_name': p['name'],
                'team':        team_name,
                'side':        side,
                'position':    entry.get('position'),
                'substitute':  entry.get('substitute'),
            }

    summary_rows = {cfg['tab']: [] for cfg in tabs_to_scrape}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            viewport={"width": 1280, "height": 900}
        )
        page = await context.new_page()

        await page.route('**/*', lambda route: route.abort()
            if any(x in route.request.url for x in [
                'googlesyndication', 'doubleclick', 'googletagmanager', 'amazon-adsystem'
            ])
            else route.continue_()
        )

        await page.goto(match_url, wait_until='domcontentloaded', timeout=60000)
        await asyncio.sleep(5)
        await page.evaluate("window.scrollTo(0, 500)")
        await asyncio.sleep(1)

        imgs = await page.locator('img[src*="sofascore.com/api/v1/player"]').all()
        seen = set()
        player_ids = []
        for img in imgs:
            src = await img.get_attribute('src')
            pid = src.split('/player/')[1].split('/')[0]
            if pid not in seen:
                seen.add(pid)
                player_ids.append(pid)

        tab_names = [cfg['tab'] for cfg in tabs_to_scrape]
        print(f"  Found {len(player_ids)} players — scraping tabs: {tab_names}")

        for i, player_id in enumerate(player_ids):
            meta = player_meta.get(player_id, {})
            print(f"  [{i+1}/{len(player_ids)}] {meta.get('player_name', player_id)}...")

            try:
                img = page.locator(f'img[src*="/player/{player_id}/image"]').first
                await img.click(force=True)
                await asyncio.sleep(2)

                for cfg in tabs_to_scrape:
                    tab_name = cfg['tab']

                    tab_btn = page.locator(f'button:has-text("{tab_name}")').first
                    if await tab_btn.count() == 0:
                        print(f"    [{tab_name}] No tab found")
                        continue

                    await tab_btn.click(force=True)
                    await asyncio.sleep(1.5)

                    svgs = await page.locator('svg').all()
                    svg_el = None
                    svg_outer = None
                    for svg in svgs:
                        box = await svg.bounding_box()
                        if box and box['width'] > 200 and box['x'] > 800 and box['height'] < 300:
                            svg_el = svg
                            svg_outer = await svg.evaluate('el => el.outerHTML')
                            break

                    if svg_outer:
                        png_path = f"{cfg['svg_dir']}/{event_id}_{player_id}.png"
                        await svg_el.screenshot(path=png_path)

                        action_rows = cfg['parse_fn'](svg_outer, event_id, player_id)
                        summary = cfg['summarize_fn'](
                            action_rows, event_id, player_id,
                            meta.get('player_name', ''),
                            meta.get('team', ''),
                            meta.get('side', ''),
                            meta.get('position', ''),
                            meta.get('substitute', ''),
                            row['season'], row['date'],
                            row['home_team'], row['away_team']
                        )
                        if summary:
                            summary_rows[tab_name].append(summary)
                            print(f"    [{tab_name}] {len(action_rows)} actions")
                    else:
                        print(f"    [{tab_name}] No SVG found")

                close_btn = page.locator('button:has-text("✕"), button:has-text("×")').first
                if await close_btn.count() > 0:
                    await close_btn.click(force=True)
                else:
                    await page.mouse.click(1213, 318)
                await asyncio.sleep(1)

            except Exception as e:
                print(f"    Error: {e}")
                await page.keyboard.press('Escape')
                await asyncio.sleep(1)

        await browser.close()

    for cfg in tabs_to_scrape:
        tab_name = cfg['tab']
        rows = summary_rows[tab_name]
        if rows:
            pd.DataFrame(rows).to_csv(filenames[tab_name], index=False)
            print(f"  [{tab_name}] Saved {len(rows)} rows → {filenames[tab_name]}")
        else:
            print(f"  [{tab_name}] No data to save")


# ── Fetch pipeline ─────────────────────────────────────────────────────────────

async def fetch_all_maps_for_dates(df_matches, start_date, end_date,
                                    statuses=('Ended', 'AET', 'AP'), overwrite=False):
    mask = (
        (df_matches['date'] >= pd.to_datetime(start_date).date()) &
        (df_matches['date'] <= pd.to_datetime(end_date).date()) &
        (df_matches['status'].isin(statuses))
    )
    subset = df_matches[mask].reset_index(drop=True)
    print(f"Scraping all maps for {len(subset)} matches ({start_date} → {end_date})\n")

    for i, row in subset.iterrows():
        print(f"[{i+1}/{len(subset)}] {row['home_team']} vs {row['away_team']} ({row['date']})")
        await scrape_all_maps(row['match_url'], row['event_id'], row, overwrite=overwrite)
        await asyncio.sleep(2)

    print(f"\nDone")
