import os
import time
import asyncio
import requests
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ── Config ───────────────────────────────────────────────────────────────────

PASS_SVG_DIR  = 'nwsl_passmap_svg'
PASS_DATA_DIR = 'nwsl_passmap_data'
for d in [PASS_SVG_DIR, PASS_DATA_DIR]:
    os.makedirs(d, exist_ok=True)

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://www.sofascore.com/"
}

# ── SVG parser ────────────────────────────────────────────────────────────────

def parse_pass_svg(svg_outer_html, event_id, player_id):
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

    passes = []

    for line in soup.find_all('line'):
        stroke = line.get('stroke', '')
        if 'success' in stroke:
            accurate = True
        elif 'error' in stroke:
            accurate = False
        else:
            continue

        x1 = norm_x(line.get('x1', 0))
        y1 = norm_y(line.get('y1', 0))
        x2 = norm_x(line.get('x2', 0))
        y2 = norm_y(line.get('y2', 0))

        style = line.get('style', '')
        try:
            dash = style.split('stroke-dasharray:')[1].strip().split(',')[0].strip()
            pass_length = round(float(dash) / 344 * 100, 1)
        except:
            pass_length = None

        dx = x2 - x1
        if dx > 5:
            direction = 'forward'
        elif dx < -5:
            direction = 'backward'
        else:
            direction = 'lateral'

        passes.append({
            'event_id':     event_id,
            'player_id':    player_id,
            'accurate':     accurate,
            'x_start':      x1,
            'y_start':      y1,
            'x_end':        x2,
            'y_end':        y2,
            'pass_length':  pass_length,
            'direction':    direction,
            'zone_start_x': zone_x(x1),
            'zone_start_y': zone_y(y1),
            'zone_end_x':   zone_x(x2),
            'zone_end_y':   zone_y(y2),
        })

    return passes


def summarize_pass_actions(passes, event_id, player_id, player_name,
                            team, side, position, substitute,
                            season, match_date, home_team, away_team):
    if not passes:
        return {}

    df = pd.DataFrame(passes)

    total      = len(df)
    accurate   = df[df['accurate'] == True]
    inaccurate = df[df['accurate'] == False]

    def zone_pct(subset, col, val):
        if len(subset) == 0: return None
        return round((subset[col] == val).sum() / len(subset), 3)

    def dir_pct(subset, val):
        if len(subset) == 0: return None
        return round((subset['direction'] == val).sum() / len(subset), 3)

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
        'passes_total':      total,
        'passes_accurate':   len(accurate),
        'passes_inaccurate': len(inaccurate),
        'pass_accuracy':     round(len(accurate) / total, 3) if total else None,
        'avg_pass_length':   round(df['pass_length'].mean(), 1) if 'pass_length' in df else None,
        'pct_forward':       dir_pct(df, 'forward'),
        'pct_backward':      dir_pct(df, 'backward'),
        'pct_lateral':       dir_pct(df, 'lateral'),
        'acc_pct_forward':   dir_pct(accurate, 'forward'),
        'acc_pct_backward':  dir_pct(accurate, 'backward'),
        'acc_pct_lateral':   dir_pct(accurate, 'lateral'),
        'origin_def_third':  zone_pct(df, 'zone_start_x', 'defensive_third'),
        'origin_mid_third':  zone_pct(df, 'zone_start_x', 'middle_third'),
        'origin_att_third':  zone_pct(df, 'zone_start_x', 'attacking_third'),
        'origin_left_wing':  zone_pct(df, 'zone_start_y', 'left_wing'),
        'origin_central':    zone_pct(df, 'zone_start_y', 'central'),
        'origin_right_wing': zone_pct(df, 'zone_start_y', 'right_wing'),
        'dest_def_third':    zone_pct(df, 'zone_end_x', 'defensive_third'),
        'dest_mid_third':    zone_pct(df, 'zone_end_x', 'middle_third'),
        'dest_att_third':    zone_pct(df, 'zone_end_x', 'attacking_third'),
        'dest_left_wing':    zone_pct(df, 'zone_end_y', 'left_wing'),
        'dest_central':      zone_pct(df, 'zone_end_y', 'central'),
        'dest_right_wing':   zone_pct(df, 'zone_end_y', 'right_wing'),
        'acc_dest_def_third':  zone_pct(accurate, 'zone_end_x', 'defensive_third'),
        'acc_dest_mid_third':  zone_pct(accurate, 'zone_end_x', 'middle_third'),
        'acc_dest_att_third':  zone_pct(accurate, 'zone_end_x', 'attacking_third'),
        'acc_dest_left_wing':  zone_pct(accurate, 'zone_end_y', 'left_wing'),
        'acc_dest_central':    zone_pct(accurate, 'zone_end_y', 'central'),
        'acc_dest_right_wing': zone_pct(accurate, 'zone_end_y', 'right_wing'),
        'progressive_passes':     int((df['x_end'] - df['x_start'] > 10).sum()),
        'progressive_pass_pct':   round((df['x_end'] - df['x_start'] > 10).sum() / total, 3) if total else None,
    }


# ── Playwright scraper ────────────────────────────────────────────────────────

async def scrape_pass_maps(match_url, event_id, row, overwrite=False):
    filename = f"{PASS_DATA_DIR}/{event_id}.csv"
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

                tab_btn = page.locator('button:has-text("Pass")').first
                if await tab_btn.count() == 0:
                    print(f"    No Pass tab")
                    await page.keyboard.press('Escape')
                    await asyncio.sleep(1)
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
                    # Save screenshot
                    png_path = f"{PASS_SVG_DIR}/{event_id}_{player_id}.png"
                    await svg_el.screenshot(path=png_path)

                    # Parse structured data
                    pass_rows = parse_pass_svg(svg_outer, event_id, player_id)
                    summary = summarize_pass_actions(
                        pass_rows, event_id, player_id,
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
                        acc = len([p for p in pass_rows if p['accurate']])
                        print(f"    {len(pass_rows)} passes ({acc} accurate) → {png_path}")
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

async def fetch_pass_maps_for_dates(df_matches, start_date, end_date,
                                     statuses=('Ended', 'AET', 'AP'), overwrite=False):
    mask = (
        (df_matches['date'] >= pd.to_datetime(start_date).date()) &
        (df_matches['date'] <= pd.to_datetime(end_date).date()) &
        (df_matches['status'].isin(statuses))
    )
    subset = df_matches[mask].reset_index(drop=True)
    print(f"Scraping pass maps for {len(subset)} matches ({start_date} → {end_date})\n")

    for i, row in subset.iterrows():
        print(f"[{i+1}/{len(subset)}] {row['home_team']} vs {row['away_team']} ({row['date']})")
        await scrape_pass_maps(row['match_url'], row['event_id'], row, overwrite=overwrite)
        await asyncio.sleep(2)

    print(f"\nDone — files in ./{PASS_DATA_DIR}/")


# ── Load all files ────────────────────────────────────────────────────────────

def load_pass_map_files(pass_dir=PASS_DATA_DIR):
    files = [f for f in os.listdir(pass_dir) if f.endswith('.csv')]
    if not files:
        print("No files found")
        return pd.DataFrame()
    dfs = [pd.read_csv(os.path.join(pass_dir, f)) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    print(f"Loaded {len(files)} files — {len(df)} total rows")
    return df
