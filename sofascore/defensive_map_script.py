import os
import time
import asyncio
import requests
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ── Config ───────────────────────────────────────────────────────────────────

DEF_SVG_DIR  = 'nwsl_defmap_svg'
DEF_DATA_DIR = 'nwsl_defmap_data'
for d in [DEF_SVG_DIR, DEF_DATA_DIR]:
    os.makedirs(d, exist_ok=True)

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://www.sofascore.com/"
}

# ── Color → action mapping ────────────────────────────────────────────────────

COLOR_MAP = {
    'var(--colors-status-success-default)':    'recovery',
    'var(--colors-rating-s80)':                'clearance',
    'var(--colors-surface-s1)':                'missed_tackle',
    'var(--colors-rating-s60)':                'tackle_won',
    'var(--colors-rating-s10)':                'block',
    'var(--colors-status-crowdsourcing-live)': 'interception',
}

# ── SVG parser ────────────────────────────────────────────────────────────────

def parse_def_svg(svg_outer_html, event_id, player_id):
    """
    Extract circle coordinates and action types from defensive map SVG.
    ViewBox is '-12 -12 344 224' — normalize cx/cy to 0-100.
    """
    soup = BeautifulSoup(svg_outer_html, 'html.parser')

    vb_x_min, vb_y_min = -12, -12
    vb_width, vb_height = 344, 224

    rows = []
    for circle in soup.find_all('circle'):
        cx   = float(circle.get('cx', 0))
        cy   = float(circle.get('cy', 0))
        fill = circle.get('fill', '')

        if 'neutral' in fill:
            continue

        x_norm = round((cx - vb_x_min) / vb_width * 100, 1)
        y_norm = round((cy - vb_y_min) / vb_height * 100, 1)

        action = COLOR_MAP.get(fill, f'unknown:{fill}')

        if x_norm < 33:
            zone_x = 'defensive_third'
        elif x_norm < 67:
            zone_x = 'middle_third'
        else:
            zone_x = 'attacking_third'

        if y_norm < 33:
            zone_y = 'left_wing'
        elif y_norm < 67:
            zone_y = 'central'
        else:
            zone_y = 'right_wing'

        rows.append({
            'event_id':  event_id,
            'player_id': player_id,
            'action':    action,
            'x_norm':    x_norm,
            'y_norm':    y_norm,
            'zone_x':    zone_x,
            'zone_y':    zone_y,
            'fill':      fill,
        })

    return rows


def summarize_def_actions(rows, event_id, player_id, player_name,
                           team, side, position, substitute, season,
                           match_date, home_team, away_team):
    """Roll up per-action rows into a single player-match summary."""
    if not rows:
        return {}

    df = pd.DataFrame(rows)

    def count(action):
        return int((df['action'] == action).sum())

    def zone_pct(action, zone_col, zone_val):
        sub = df[df['action'] == action]
        if len(sub) == 0:
            return None
        return round((sub[zone_col] == zone_val).sum() / len(sub), 3)

    total_actions = len(df)

    return {
        'event_id':       event_id,
        'season':         season,
        'date':           match_date,
        'home_team':      home_team,
        'away_team':      away_team,
        'team':           team,
        'side':           side,
        'player_id':      player_id,
        'player_name':    player_name,
        'position':       position,
        'substitute':     substitute,
        'tackle_won':     count('tackle_won'),
        'missed_tackle':  count('missed_tackle'),
        'interception':   count('interception'),
        'clearance':      count('clearance'),
        'block':          count('block'),
        'recovery':       count('recovery'),
        'total_def_actions': total_actions,
        'tackle_success': round(count('tackle_won') / max(count('tackle_won') + count('missed_tackle'), 1), 3),
        'pct_def_third':  round((df['zone_x'] == 'defensive_third').sum() / total_actions, 3),
        'pct_mid_third':  round((df['zone_x'] == 'middle_third').sum() / total_actions, 3),
        'pct_att_third':  round((df['zone_x'] == 'attacking_third').sum() / total_actions, 3),
        'pct_left_wing':  round((df['zone_y'] == 'left_wing').sum() / total_actions, 3),
        'pct_central':    round((df['zone_y'] == 'central').sum() / total_actions, 3),
        'pct_right_wing': round((df['zone_y'] == 'right_wing').sum() / total_actions, 3),
        'tackle_def_third':    zone_pct('tackle_won',   'zone_x', 'defensive_third'),
        'tackle_mid_third':    zone_pct('tackle_won',   'zone_x', 'middle_third'),
        'tackle_att_third':    zone_pct('tackle_won',   'zone_x', 'attacking_third'),
        'intercept_def_third': zone_pct('interception', 'zone_x', 'defensive_third'),
        'intercept_mid_third': zone_pct('interception', 'zone_x', 'middle_third'),
        'intercept_att_third': zone_pct('interception', 'zone_x', 'attacking_third'),
        'recovery_def_third':  zone_pct('recovery',     'zone_x', 'defensive_third'),
        'recovery_mid_third':  zone_pct('recovery',     'zone_x', 'middle_third'),
        'recovery_att_third':  zone_pct('recovery',     'zone_x', 'attacking_third'),
    }


# ── Playwright scraper ────────────────────────────────────────────────────────

async def scrape_def_maps(match_url, event_id, row, overwrite=False):
    filename = f"{DEF_DATA_DIR}/{event_id}.csv"
    if os.path.exists(filename) and not overwrite:
        print(f"  Skipping {event_id} — already exists")
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

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            viewport={"width": 1280, "height": 900}
        )
        page = await context.new_page()

        await page.route('**/*', lambda route: route.abort()
            if any(x in route.request.url for x in ['googlesyndication', 'doubleclick', 'googletagmanager', 'amazon-adsystem'])
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

        print(f"  Found {len(player_ids)} players")
        summary_rows = []

        for i, player_id in enumerate(player_ids):
            meta = player_meta.get(player_id, {})
            print(f"  [{i+1}/{len(player_ids)}] {meta.get('player_name', player_id)}...")

            try:
                img = page.locator(f'img[src*="/player/{player_id}/image"]').first
                await img.click(force=True)
                await asyncio.sleep(2)

                tab_btn = page.locator('button:has-text("Def")').first
                if await tab_btn.count() == 0:
                    print(f"    No Def tab")
                    await page.keyboard.press('Escape')
                    await asyncio.sleep(1)
                    continue

                await tab_btn.click(force=True)
                await asyncio.sleep(1.5)

                svgs = await page.locator('svg').all()
                svg_outer = None
                for svg in svgs:
                    box = await svg.bounding_box()
                    if box and box['width'] > 200 and box['x'] > 800 and box['height'] < 300:
                        svg_outer = await svg.evaluate('el => el.outerHTML')
                        break

                if svg_outer:
                    # Save screenshot
                    png_path = f"{DEF_SVG_DIR}/{event_id}_{player_id}.png"
                    await svg.screenshot(path=png_path)

                    # Parse structured data
                    action_rows = parse_def_svg(svg_outer, event_id, player_id)
                    summary = summarize_def_actions(
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
                        summary_rows.append(summary)
                        print(f"    {len(action_rows)} actions parsed → {png_path}")
                else:
                    print(f"    No SVG found")

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

        if summary_rows:
            pd.DataFrame(summary_rows).to_csv(filename, index=False)
            print(f"  Saved {len(summary_rows)} rows → {filename}")
        else:
            print(f"  No data to save")


# ── Fetch pipeline ────────────────────────────────────────────────────────────

async def fetch_def_maps_for_dates(df_matches, start_date, end_date,
                                    statuses=('Ended', 'AET', 'AP'), overwrite=False):
    mask = (
        (df_matches['date'] >= pd.to_datetime(start_date).date()) &
        (df_matches['date'] <= pd.to_datetime(end_date).date()) &
        (df_matches['status'].isin(statuses))
    )
    subset = df_matches[mask].reset_index(drop=True)
    print(f"Scraping def maps for {len(subset)} matches ({start_date} → {end_date})\n")

    for i, row in subset.iterrows():
        print(f"[{i+1}/{len(subset)}] {row['home_team']} vs {row['away_team']} ({row['date']})")
        await scrape_def_maps(row['match_url'], row['event_id'], row, overwrite=overwrite)
        await asyncio.sleep(2)

    print(f"\nDone — files in ./{DEF_DATA_DIR}/")


# ── Load all files ────────────────────────────────────────────────────────────

def load_def_map_files(def_dir=DEF_DATA_DIR):
    files = [f for f in os.listdir(def_dir) if f.endswith('.csv')]
    if not files:
        print("No files found")
        return pd.DataFrame()
    dfs = [pd.read_csv(os.path.join(def_dir, f)) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    print(f"Loaded {len(files)} files — {len(df)} total rows")
    return df
