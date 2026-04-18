import os
import asyncio
from curl_cffi import requests
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

async def scrape_all_maps(match_url, event_id, row, overwrite=False, game_num=None, total_games=None):
    filenames = {cfg['tab']: f"{cfg['data_dir']}/{event_id}.csv" for cfg in TAB_CONFIG}

    # Load existing CSVs to know which player IDs are already done per tab
    existing_dfs = {}
    done_ids = {}
    for cfg in TAB_CONFIG:
        tab = cfg['tab']
        if not overwrite and os.path.exists(filenames[tab]):
            df_ex = pd.read_csv(filenames[tab])
            existing_dfs[tab] = df_ex
            done_ids[tab] = set(df_ex['player_id'].astype(str))
        else:
            existing_dfs[tab] = pd.DataFrame()
            done_ids[tab] = set()

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
                'player_name':   p['name'],
                'team':          team_name,
                'side':          side,
                'position':      entry.get('position'),
                'substitute':    entry.get('substitute'),
                'minutes_played': entry.get('statistics', {}).get('minutesPlayed', 0),
            }

    new_rows = {cfg['tab']: [] for cfg in TAB_CONFIG}

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

        # Append #id:{event_id} so Sofascore's SPA loads the correct historical
        # match when two events between the same teams share the same customId.
        nav_url = f"{match_url}#id:{event_id}"
        await page.goto(nav_url, wait_until='domcontentloaded', timeout=60000)
        await page.wait_for_selector(
            'img[src*="sofascore.com/api/v1/player"]', timeout=20000
        )

        # Ensure the Lineups tab is active so the pitch graphic renders.
        # Some matches open on a different default tab, hiding starter images.
        lineups_tab = page.locator('a:has-text("Lineups"), button:has-text("Lineups")').first
        if await lineups_tab.count() > 0:
            print(f"  Clicking Lineups tab")
            await lineups_tab.click()
            await asyncio.sleep(2.0)

        # Scroll through full page so all player images lazy-load
        scroll_y = 0
        while True:
            await page.evaluate(f"window.scrollTo(0, {scroll_y})")
            await asyncio.sleep(0.3)
            page_height = await page.evaluate("document.body.scrollHeight")
            scroll_y += 600
            if scroll_y >= page_height:
                break
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(2.0)  # allow pitch graphic to fully render after scroll

        # Collect all player images. For each player, prefer the pitch image (non-nav-
        # linked) over the bench/list image (nav-linked), because only the pitch image
        # reliably opens the stats modal. A player can appear in both places if they
        # played (pitch) and are also listed in the subs bench section.
        imgs = await page.locator('img[src*="sofascore.com/api/v1/player"]').all()
        # pid -> best img element: non-nav-linked takes priority
        best_img = {}  # pid -> img element
        nav_linked = set()  # pids where only a nav-linked image was found
        for img in imgs:
            src = await img.get_attribute('src')
            pid = src.split('/player/')[1].split('/')[0]
            in_link = await img.evaluate(
                'el => { const a = el.closest("a"); return a ? a.href : ""; }'
            )
            is_nav = '/player/' in (in_link or '') and '/image' not in (in_link or '')
            if pid not in best_img:
                best_img[pid] = img
                if is_nav:
                    nav_linked.add(pid)
            elif pid in nav_linked and not is_nav:
                # We previously only had a nav-linked image; now we found a better one
                best_img[pid] = img
                nav_linked.discard(pid)
        player_ids = list(best_img.keys())

        # Filter: keep players in lineups with minutes_played != 0.
        # If minutes_played is missing/None, keep them (data may be absent for starters).
        # Only exclude players explicitly recorded as 0 minutes (unused subs).
        def _played(pid):
            meta = player_meta.get(pid)
            if meta is None:
                return True  # not in lineups API — keep (may still have map data)
            mp = meta.get('minutes_played')
            if mp is None:
                return True  # no stat recorded — assume played
            return mp != 0

        before = len(player_ids)
        player_ids = [pid for pid in player_ids if _played(pid)]
        print(f"  {len(player_ids)} players kept ({before - len(player_ids)} unused subs removed)")

        # Check if there's actually anything to do
        any_work = any(
            pid not in done_ids[cfg['tab']]
            for pid in player_ids
            for cfg in TAB_CONFIG
        )
        if not any_work:
            print(f"  Skipping {event_id} — all players already scraped for all tabs")
            await browser.close()
            return

        print(f"  Found {len(player_ids)} players")

        for i, player_id in enumerate(player_ids):
            meta = player_meta.get(player_id, {})

            # Which tabs still need this player?
            tabs_needed = [cfg for cfg in TAB_CONFIG if player_id not in done_ids[cfg['tab']]]
            if not tabs_needed:
                print(f"  [{i+1}/{len(player_ids)}] {meta.get('player_name', player_id)} — already done")
                continue

            game_prefix = f"Game {game_num}/{total_games} " if game_num else ""
            print(f"  {game_prefix}[{i+1}/{len(player_ids)}] {meta.get('player_name', player_id)}...")

            try:
                img = best_img[player_id]
                await img.click(force=True)
                await asyncio.sleep(0.5)

                # If click navigated to a player profile page, go back
                if '/player/' in page.url and str(event_id) not in page.url:
                    print(f"    Navigated to player page — going back")
                    await page.go_back(wait_until='domcontentloaded', timeout=15000)
                    await asyncio.sleep(0.5)
                    print(f"    Skipping {meta.get('player_name', player_id)} (no modal trigger found)")
                    continue

                await asyncio.sleep(1.0)  # wait for modal to render

                for cfg in tabs_needed:
                    tab_name = cfg['tab']

                    # Try data-testid first (most reliable), fall back to button text
                    testid = f'[data-testid="tab-{tab_name.lower()}"]'
                    all_testid = page.locator(testid)
                    all_btn_text = page.locator(f'button:has-text("{tab_name}")')
                    testid_count = await all_testid.count()
                    btn_count = await all_btn_text.count()
                    print(f"    [{tab_name}] testid={testid_count} btn_text={btn_count}")

                    if testid_count > 1:
                        # >1 means modal one is present (first is on main page)
                        tab_btn = all_testid.last
                    elif btn_count > 0:
                        tab_btn = all_btn_text.last
                    else:
                        print(f"    [{tab_name}] No tab found")
                        continue

                    await tab_btn.click(force=True)
                    try:
                        await page.wait_for_function(
                            """() => {
                                const svgs = document.querySelectorAll('svg');
                                return Array.from(svgs).some(s => {
                                    const b = s.getBoundingClientRect();
                                    return b.width > 200 && b.x > 800 && b.height < 300;
                                });
                            }""",
                            timeout=4000
                        )
                    except Exception:
                        pass  # SVG may not exist for this player/tab

                    # Normalize pitch orientation to >>> (attacking right).
                    # If the left-chevron button is present the pitch is flipped — click to reset.
                    LEFT_CHEVRON_PATH = 'm10 14 1.41-1.41L6.83 8l4.58-4.59L10 2 4 8z'
                    flip_btn = page.locator(f'button svg path[d="{LEFT_CHEVRON_PATH}"]').locator('..').locator('..').first
                    if await flip_btn.count() > 0:
                        print(f"    [{tab_name}] Flipping pitch orientation")
                        await flip_btn.click(force=True)
                        await asyncio.sleep(0.8)

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

                        action_rows = cfg['parse_fn'](svg_outer, event_id, player_id, side=meta.get('side', 'home'))
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
                            new_rows[tab_name].append(summary)
                            print(f"    [{tab_name}] {len(action_rows)} actions")
                    else:
                        print(f"    [{tab_name}] No SVG found")

                close_btn = page.locator('button:has-text("✕"), button:has-text("×")').first
                if await close_btn.count() > 0:
                    await close_btn.click(force=True)
                else:
                    await page.mouse.click(1213, 318)
                await asyncio.sleep(0.5)

            except Exception as e:
                print(f"    Error: {e}")
                await page.keyboard.press('Escape')
                await asyncio.sleep(0.5)

        await browser.close()

    # Merge new rows with existing and save
    for cfg in TAB_CONFIG:
        tab_name = cfg['tab']
        if not new_rows[tab_name]:
            continue
        df_new = pd.DataFrame(new_rows[tab_name])
        df_ex = existing_dfs[tab_name]
        df_out = pd.concat([df_ex, df_new], ignore_index=True) if not df_ex.empty else df_new
        df_out.to_csv(filenames[tab_name], index=False)
        print(f"  [{tab_name}] Saved {len(new_rows[tab_name])} new rows → {filenames[tab_name]}")


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

    for game_num, (_, row) in enumerate(subset.iterrows(), 1):
        print(f"[Game {game_num}/{len(subset)}] {row['home_team']} vs {row['away_team']} ({row['date']}) — id: {row['event_id']}")
        await scrape_all_maps(row['match_url'], row['event_id'], row, overwrite=overwrite, game_num=game_num, total_games=len(subset))
        await asyncio.sleep(2)

    print(f"\nDone")
