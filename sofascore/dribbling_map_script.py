import os
import time
import asyncio
from curl_cffi import requests
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ── Config ───────────────────────────────────────────────────────────────────

DRIB_SVG_DIR  = 'nwsl_dribmap_svg'
DRIB_DATA_DIR = 'nwsl_dribmap_data'
for d in [DRIB_SVG_DIR, DRIB_DATA_DIR]:
    os.makedirs(d, exist_ok=True)

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://www.sofascore.com/"
}

# ── SVG parser ────────────────────────────────────────────────────────────────

def parse_drib_svg(svg_outer_html, event_id, player_id):
    soup = BeautifulSoup(svg_outer_html, 'html.parser')

    vb_x_min, vb_y_min = -12, -12
    vb_width, vb_height = 344, 224

    def norm_x(val):
        return round((float(val) - vb_x_min) / vb_width * 100, 1)

    def norm_y(val):
        return round((float(val) - vb_y_min) / vb_height * 100, 1)

    def zone_x(x):
        if x < 33:   return 'defensive_third'
        elif x < 67: return 'middle_third'
        else:        return 'attacking_third'

    def zone_y(y):
        if y < 33:   return 'left_wing'
        elif y < 67: return 'central'
        else:        return 'right_wing'

    dribbles = []
    carries  = []

    for circle in soup.find_all('circle'):
        fill = circle.get('fill', '')
        if 'neutral' in fill:
            continue
        cx = float(circle.get('cx', 0))
        cy = float(circle.get('cy', 0))
        x  = norm_x(cx)
        y  = norm_y(cy)

        if fill == 'var(--colors-status-success-default)':
            action = 'dribble_won'
        elif fill == 'var(--colors-status-error-default)':
            action = 'dribble_lost'
        else:
            action = f'unknown:{fill}'

        dribbles.append({
            'event_id':  event_id,
            'player_id': player_id,
            'type':      'dribble',
            'action':    action,
            'x_start':   x,
            'y_start':   y,
            'x_end':     None,
            'y_end':     None,
            'zone_x':    zone_x(x),
            'zone_y':    zone_y(y),
        })

    for line in soup.find_all('line'):
        stroke = line.get('stroke', '')
        if 'neutrals-n-lv1' not in stroke:
            continue
        x1 = norm_x(line.get('x1', 0))
        y1 = norm_y(line.get('y1', 0))
        x2 = norm_x(line.get('x2', 0))
        y2 = norm_y(line.get('y2', 0))

        carries.append({
            'event_id':  event_id,
            'player_id': player_id,
            'type':      'carry',
            'action':    'carry',
            'x_start':   x1,
            'y_start':   y1,
            'x_end':     x2,
            'y_end':     y2,
            'zone_x':    zone_x(x1),
            'zone_y':    zone_y(y1),
        })

    return dribbles + carries


def summarize_drib_actions(rows, event_id, player_id, player_name,
                            team, side, position, substitute,
                            season, match_date, home_team, away_team):
    if not rows:
        return {}

    df = pd.DataFrame(rows)

    dribbles = df[df['type'] == 'dribble']
    carries  = df[df['type'] == 'carry']

    drib_won   = int((dribbles['action'] == 'dribble_won').sum())
    drib_lost  = int((dribbles['action'] == 'dribble_lost').sum())
    drib_total = drib_won + drib_lost
    carry_count = len(carries)

    def zone_pct(subset, zone_col, zone_val):
        if len(subset) == 0: return None
        return round((subset[zone_col] == zone_val).sum() / len(subset), 3)

    won = dribbles[dribbles['action'] == 'dribble_won']

    return {
        'event_id':    event_id,
        'season':      season,
        'date':        match_date,
        'home_team':   home_team,
        'away_team':   away_team,
        'team':        team,
        'side':        side,
        'player_id':   player_id,
        'player_name': player_name,
        'position':    position,
        'substitute':  substitute,
        'dribbles_won':     drib_won,
        'dribbles_lost':    drib_lost,
        'dribbles_total':   drib_total,
        'dribble_success':  round(drib_won / drib_total, 3) if drib_total else None,
        'carry_segments':   carry_count,
        'drib_def_third':   zone_pct(dribbles, 'zone_x', 'defensive_third'),
        'drib_mid_third':   zone_pct(dribbles, 'zone_x', 'middle_third'),
        'drib_att_third':   zone_pct(dribbles, 'zone_x', 'attacking_third'),
        'drib_left_wing':   zone_pct(dribbles, 'zone_y', 'left_wing'),
        'drib_central':     zone_pct(dribbles, 'zone_y', 'central'),
        'drib_right_wing':  zone_pct(dribbles, 'zone_y', 'right_wing'),
        'carry_def_third':  zone_pct(carries, 'zone_x', 'defensive_third'),
        'carry_mid_third':  zone_pct(carries, 'zone_x', 'middle_third'),
        'carry_att_third':  zone_pct(carries, 'zone_x', 'attacking_third'),
        'carry_left_wing':  zone_pct(carries, 'zone_y', 'left_wing'),
        'carry_central':    zone_pct(carries, 'zone_y', 'central'),
        'carry_right_wing': zone_pct(carries, 'zone_y', 'right_wing'),
        'drib_won_def_third': zone_pct(won, 'zone_x', 'defensive_third'),
        'drib_won_mid_third': zone_pct(won, 'zone_x', 'middle_third'),
        'drib_won_att_third': zone_pct(won, 'zone_x', 'attacking_third'),
    }


# ── Playwright scraper ────────────────────────────────────────────────────────

async def scrape_drib_maps(match_url, event_id, row, overwrite=False):
    filename = f"{DRIB_DATA_DIR}/{event_id}.csv"
    if os.path.exists(filename) and not overwrite:
        print(f"  Skipping {event_id} — already exists")
        return

    resp = requests.get(
        f"https://api.sofascore.com/api/v1/event/{event_id}/lineups",
        headers=headers, impersonate="chrome"
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

                tab_btn = page.locator('button:has-text("Drib")').first
                if await tab_btn.count() == 0:
                    print(f"    No Drib tab")
                    await page.keyboard.press('Escape')
                    await asyncio.sleep(1)
                    continue

                await tab_btn.click(force=True)
                await asyncio.sleep(1.5)

                # Normalize pitch orientation to >>> (attacking right).
                # If the left-chevron path is present, pitch is flipped — click to reset.
                LEFT_CHEVRON_PATH = 'm10 14 1.41-1.41L6.83 8l4.58-4.59L10 2 4 8z'
                flip_btn = page.locator(f'button svg path[d="{LEFT_CHEVRON_PATH}"]').locator('..').locator('..').first
                if await flip_btn.count() > 0:
                    await flip_btn.click(force=True)
                    await asyncio.sleep(0.5)

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
                    # Save screenshot
                    png_path = f"{DRIB_SVG_DIR}/{event_id}_{player_id}.png"
                    await svg_el.screenshot(path=png_path)

                    # Parse structured data
                    action_rows = parse_drib_svg(svg_outer, event_id, player_id)
                    summary = summarize_drib_actions(
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
                        drb = len([r for r in action_rows if r['type'] == 'dribble'])
                        cry = len([r for r in action_rows if r['type'] == 'carry'])
                        print(f"    {drb} dribbles, {cry} carry segments → {png_path}")
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

async def fetch_drib_maps_for_dates(df_matches, start_date, end_date,
                                     statuses=('Ended', 'AET', 'AP'), overwrite=False):
    mask = (
        (df_matches['date'] >= pd.to_datetime(start_date).date()) &
        (df_matches['date'] <= pd.to_datetime(end_date).date()) &
        (df_matches['status'].isin(statuses))
    )
    subset = df_matches[mask].reset_index(drop=True)
    print(f"Scraping dribble maps for {len(subset)} matches ({start_date} → {end_date})\n")

    for i, row in subset.iterrows():
        print(f"[{i+1}/{len(subset)}] {row['home_team']} vs {row['away_team']} ({row['date']})")
        await scrape_drib_maps(row['match_url'], row['event_id'], row, overwrite=overwrite)
        await asyncio.sleep(2)

    print(f"\nDone — files in ./{DRIB_DATA_DIR}/")


# ── Load all files ────────────────────────────────────────────────────────────

def load_drib_map_files(drib_dir=DRIB_DATA_DIR):
    files = [f for f in os.listdir(drib_dir) if f.endswith('.csv')]
    if not files:
        print("No files found")
        return pd.DataFrame()
    dfs = [pd.read_csv(os.path.join(drib_dir, f)) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    print(f"Loaded {len(files)} files — {len(df)} total rows")
    return df
