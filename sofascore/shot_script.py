import os
import time
import random
from curl_cffi import requests
import pandas as pd

# ── Config ───────────────────────────────────────────────────────────────────

SHOT_TABLE_DIR = 'nwsl_shot_tables'
os.makedirs(SHOT_TABLE_DIR, exist_ok=True)

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://www.sofascore.com/"
}


# ── API helpers ───────────────────────────────────────────────────────────────

def safe_get(url, headers, max_retries=3, base_sleep=0.3):
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers, impersonate="chrome")
        if resp.status_code == 200:
            return resp
        elif resp.status_code == 429:
            wait = (2 ** attempt) + random.uniform(0, 1)
            print(f"  Rate limited — waiting {wait:.1f}s")
            time.sleep(wait)
        elif resp.status_code == 404:
            return resp
        else:
            time.sleep(base_sleep * (attempt + 1))
    return None


# ── Shot scraping ─────────────────────────────────────────────────────────────

def get_shotmap_table(event_id, home_team, away_team, season, match_date):
    resp = safe_get(
        f"https://api.sofascore.com/api/v1/event/{event_id}/shotmap",
        headers=headers
    )
    if resp is None or resp.status_code != 200:
        return []

    shots = resp.json().get('shotmap', [])
    rows = []

    for shot in shots:
        player = shot.get('player', {})
        pc     = shot.get('playerCoordinates', {})
        gmc    = shot.get('goalMouthCoordinates', {})
        draw   = shot.get('draw', {})

        rows.append({
            # Match context
            'event_id':            event_id,
            'season':              season,
            'date':                match_date,
            'home_team':           home_team,
            'away_team':           away_team,
            'team':                home_team if shot.get('isHome') else away_team,
            'side':                'home' if shot.get('isHome') else 'away',
            # Player
            'player_id':           player.get('id'),
            'player_name':         player.get('name'),
            'position':            player.get('position'),
            # Shot details
            'shot_type':           shot.get('shotType'),
            'situation':           shot.get('situation'),
            'body_part':           shot.get('bodyPart'),
            'goal_mouth_location': shot.get('goalMouthLocation'),
            'time':                shot.get('time'),
            'added_time':          shot.get('addedTime', 0),
            'time_seconds':        shot.get('timeSeconds'),
            # Player position on pitch
            'player_x':            pc.get('x'),
            'player_y':            pc.get('y'),
            # Goal mouth coordinates
            'goal_mouth_x':        gmc.get('x'),
            'goal_mouth_y':        gmc.get('y'),
            'goal_mouth_z':        gmc.get('z'),
            # Draw coordinates (trajectory)
            'draw_start_x':        draw.get('start', {}).get('x'),
            'draw_start_y':        draw.get('start', {}).get('y'),
            'draw_end_x':          draw.get('end', {}).get('x'),
            'draw_end_y':          draw.get('end', {}).get('y'),
            # Derived booleans
            'is_goal':             shot.get('shotType') == 'goal',
            'is_on_target':        shot.get('shotType') in ('goal', 'save'),
            'is_blocked':          shot.get('shotType') == 'block',
        })

    return rows


# ── Fetch and write per-match files ───────────────────────────────────────────

def fetch_shot_tables_for_dates(df_matches, start_date, end_date,
                                 statuses=('Ended', 'AET', 'AP'), overwrite=False):
    mask = (
        (df_matches['date'] >= pd.to_datetime(start_date).date()) &
        (df_matches['date'] <= pd.to_datetime(end_date).date()) &
        (df_matches['status'].isin(statuses))
    )
    subset = df_matches[mask].reset_index(drop=True)
    print(f"Fetching shot tables for {len(subset)} matches ({start_date} → {end_date})\n")

    for i, row in subset.iterrows():
        event_id = row['event_id']
        filename = f"{SHOT_TABLE_DIR}/{event_id}.csv"

        if os.path.exists(filename) and not overwrite:
            print(f"[{i+1}/{len(subset)}] Skipping {event_id} — already exists")
            continue

        print(f"[{i+1}/{len(subset)}] {row['home_team']} vs {row['away_team']} ({row['date']})")
        rows = get_shotmap_table(
            row['event_id'], row['home_team'], row['away_team'],
            row['season'], row['date']
        )

        if rows:
            pd.DataFrame(rows).to_csv(filename, index=False)
            print(f"  Saved {len(rows)} shots → {filename}")
        else:
            print(f"  No data returned, skipping")

        time.sleep(0.5)

    print(f"\nDone — files in ./{SHOT_TABLE_DIR}/")


# ── Load all files ────────────────────────────────────────────────────────────

def load_all_shot_tables(shot_dir=SHOT_TABLE_DIR):
    files = [f for f in os.listdir(shot_dir) if f.endswith('.csv')]
    if not files:
        print("No files found")
        return pd.DataFrame()
    dfs = [pd.read_csv(os.path.join(shot_dir, f)) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    print(f"Loaded {len(files)} files — {len(df)} total shots")
    return df
