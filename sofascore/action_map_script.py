import os
import asyncio
from playwright.async_api import async_playwright

# ── Config ───────────────────────────────────────────────────────────────────

PASS_DIR  = 'nwsl_passmaps'
DEF_DIR   = 'nwsl_defmaps'
DRIB_DIR  = 'nwsl_dribmaps'
SHOT_DIR  = 'nwsl_shotmaps_screenshots'

for d in [PASS_DIR, DEF_DIR, DRIB_DIR, SHOT_DIR]:
    os.makedirs(d, exist_ok=True)


# ── Core scraper ─────────────────────────────────────────────────────────────

async def screenshot_all_maps(match_url, event_id, tab='Pass', output_dir=None):
    if output_dir is None:
        output_dir = f'nwsl_{tab.lower()}maps'
    os.makedirs(output_dir, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            viewport={"width": 1280, "height": 900}
        )
        page = await context.new_page()

        # Block ads to prevent click interception
        await page.route('**/*', lambda route: route.abort()
            if any(x in route.request.url for x in [
                'googlesyndication', 'doubleclick',
                'googletagmanager', 'amazon-adsystem'
            ])
            else route.continue_()
        )

        await page.goto(match_url, wait_until='domcontentloaded', timeout=60000)
        await asyncio.sleep(5)
        await page.evaluate("window.scrollTo(0, 500)")
        await asyncio.sleep(1)

        # Get unique player IDs from images
        imgs = await page.locator('img[src*="sofascore.com/api/v1/player"]').all()
        seen = set()
        player_ids = []
        for img in imgs:
            src = await img.get_attribute('src')
            player_id = src.split('/player/')[1].split('/')[0]
            if player_id not in seen:
                seen.add(player_id)
                player_ids.append(player_id)

        print(f"Found {len(player_ids)} unique players — scraping '{tab}' maps\n")

        for i, player_id in enumerate(player_ids):
            filename = f"{output_dir}/{event_id}_{player_id}.png"
            if os.path.exists(filename):
                print(f"[{i+1}/{len(player_ids)}] Skipping {player_id} — already exists")
                continue

            print(f"[{i+1}/{len(player_ids)}] Player {player_id}...")

            try:
                # Click player image
                img = page.locator(f'img[src*="/player/{player_id}/image"]').first
                await img.click(force=True)
                await asyncio.sleep(2)

                # Click the requested tab
                tab_btn = page.locator(f'button:has-text("{tab}")').first
                if await tab_btn.count() > 0:
                    await tab_btn.click(force=True)
                    await asyncio.sleep(1.5)
                else:
                    print(f"  '{tab}' tab not found — skipping")
                    await page.keyboard.press('Escape')
                    await asyncio.sleep(1)
                    continue

                # Find the map SVG — largest one on right side of screen
                svgs = await page.locator('svg').all()
                map_el = None
                for svg in svgs:
                    box = await svg.bounding_box()
                    if box and box['width'] > 200 and box['x'] > 800 and box['height'] < 300:
                        map_el = svg
                        break

                if map_el:
                    await map_el.screenshot(path=filename)
                    print(f"  Saved → {filename}")
                else:
                    print(f"  Map SVG not found")

                # Close popup
                close_btn = page.locator('button:has-text("✕"), button:has-text("×")').first
                if await close_btn.count() > 0:
                    await close_btn.click(force=True)
                else:
                    await page.mouse.click(1213, 318)
                await asyncio.sleep(1)

            except Exception as e:
                print(f"  Error: {e}")
                await page.keyboard.press('Escape')
                await asyncio.sleep(1)

        await browser.close()
        print(f"\nDone — screenshots in ./{output_dir}/")
