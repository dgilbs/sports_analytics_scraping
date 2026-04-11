import os
import time
import random
import requests
import pandas as pd

# ── Config ───────────────────────────────────────────────────────────────────

HEATMAP_DIR = 'nwsl_heatmap_zones'
os.makedirs(HEATMAP_DIR, exist_ok=True)

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://www.sofascore.com/"
}

SEASONS = {
    '2025': 71412,
    '2026': 88711,
}

# ── API helpers ───────────────────────────────────────────────────────────────

def safe_get(url, headers, max_retries=3, base_sleep=0.3):
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            return resp
        elif resp.status_code == 429:
            wait = (2 ** attempt) + random.uniform(0, 1)
            print(f"  Rate limited — waiting {wait:.1f}s before retry {attempt+1}/{max_retries}")
            time.sleep(wait)
        elif resp.status_code == 404:
            return resp
        else:
            wait = base_sleep * (attempt + 1)
            print(f"  HTTP {resp.status_code} — waiting {wait:.1f}s before retry {attempt+1}/{max_retries}")
            time.sleep(wait)
    print(f"  Failed after {max_retries} retries: {url}")
    return None


# ── Events ────────────────────────────────────────────────────────────────────

def get_season_events(tournament_id, season_id):
    all_events = []
    for page in range(0, 20):
        resp = safe_get(
            f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/season/{season_id}/events/last/{page}",
            headers=headers
        )
        if resp is None or resp.status_code != 200: break
        events = resp.json().get('events', [])
        if not events: break
        all_events.extend(events)
        time.sleep(0.2)
    for page in range(0, 5):
        resp = safe_get(
            f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/season/{season_id}/events/next/{page}",
            headers=headers
        )
        if resp is None or resp.status_code != 200: break
        events = resp.json().get('events', [])
        if not events: break
        all_events.extend(events)
        time.sleep(0.2)
    return all_events


def build_events_df(seasons):
    rows = []
    for year, season_id in seasons.items():
        events = get_season_events(1690, season_id)
        for e in events:
            rows.append({
                'event_id':         e['id'],
                'season':           year,
                'round':            e.get('roundInfo', {}).get('round'),
                'home_team':        e['homeTeam']['name'],
                'away_team':        e['awayTeam']['name'],
                'home_score':       e['homeScore'].get('current'),
                'away_score':       e['awayScore'].get('current'),
                'winner_code':      e.get('winnerCode'),
                'status':           e['status']['description'],
                'start_timestamp':  e['startTimestamp'],
                'has_player_stats': e.get('hasEventPlayerStatistics', False),
            })
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['start_timestamp'], unit='s').dt.date
    df = df.sort_values('date').reset_index(drop=True)
    return df


# ── Zone scoring ──────────────────────────────────────────────────────────────

def zone_scores(points):
    """
    Sofascore coordinates are already normalized — no flipping needed.
    x=0 is defensive end for both teams, x=100 is attacking end.
    y=0 is one wing, y=50 is central, y=100 is other wing.
    """
    if not points:
        return {
            'defensive_third': None,
            'middle_third':    None,
            'attacking_third': None,
            'left_wing':       None,
            'central':         None,
            'right_wing':      None,
        }

    total = len(points)

    defensive = sum(1 for p in points if p['x'] < 33) / total
    middle    = sum(1 for p in points if 33 <= p['x'] < 67) / total
    attacking = sum(1 for p in points if p['x'] >= 67) / total

    left_wing  = sum(1 for p in points if p['y'] < 33) / total
    central    = sum(1 for p in points if 33 <= p['y'] < 67) / total
    right_wing = sum(1 for p in points if p['y'] >= 67) / total

    return {
        'defensive_third': round(defensive, 3),
        'middle_third':    round(middle, 3),
        'attacking_third': round(attacking, 3),
        'left_wing':       round(left_wing, 3),
        'central':         round(central, 3),
        'right_wing':      round(right_wing, 3),
    }


def get_heatmap(event_id, player_id):
    resp = safe_get(
        f"https://api.sofascore.com/api/v1/event/{event_id}/player/{player_id}/heatmap",
        headers=headers
    )
    if resp and resp.status_code == 200:
        return resp.json().get('heatmap', [])
    return []


# ── Per-match heatmap zones ───────────────────────────────────────────────────

def get_match_heatmap_zones(row):
    event_id  = row['event_id']
    home_team = row['home_team']
    away_team = row['away_team']
    date      = row['date']

    resp = safe_get(
        f"https://api.sofascore.com/api/v1/event/{event_id}/lineups",
        headers=headers
    )
    if resp is None or resp.status_code != 200:
        print(f"  Skipping event {event_id} — no lineups")
        return []

    lineups = resp.json()
    rows = []

    for side, team_name in [('home', home_team), ('away', away_team)]:
        for entry in lineups.get(side, {}).get('players', []):
            player    = entry['player']
            player_id = player['id']

            points = get_heatmap(event_id, player_id)
            time.sleep(0.2 + random.uniform(0, 0.1))

            zones = zone_scores(points)

            row_data = {
                'event_id':    event_id,
                'season':      row['season'],
                'date':        date,
                'home_team':   home_team,
                'away_team':   away_team,
                'team':        team_name,
                'side':        side,
                'player_id':   player_id,
                'player_name': player['name'],
                'position':    entry.get('position'),
                'substitute':  entry.get('substitute'),
                'touch_count': len(points),
            }
            row_data.update(zones)
            rows.append(row_data)

    time.sleep(0.5)
    return rows


# ── Fetch and write per-match files ───────────────────────────────────────────

def fetch_heatmaps_for_dates(df_matches, start_date, end_date,
                              statuses=('Ended', 'AET', 'AP'), overwrite=False):
    mask = (
        (df_matches['date'] >= pd.to_datetime(start_date).date()) &
        (df_matches['date'] <= pd.to_datetime(end_date).date()) &
        (df_matches['status'].isin(statuses))
    )
    subset = df_matches[mask].reset_index(drop=True)
    print(f"Fetching heatmaps for {len(subset)} matches ({start_date} → {end_date})\n")

    for i, row in subset.iterrows():
        event_id = row['event_id']
        filename = f"{HEATMAP_DIR}/{event_id}.csv"

        if os.path.exists(filename) and not overwrite:
            print(f"[{i+1}/{len(subset)}] Skipping {event_id} — already exists")
            continue

        print(f"[{i+1}/{len(subset)}] {row['home_team']} vs {row['away_team']} ({row['date']})")
        rows = get_match_heatmap_zones(row)

        if rows:
            pd.DataFrame(rows).to_csv(filename, index=False)
            print(f"  Saved {len(rows)} rows → {filename}")
        else:
            print(f"  No data returned, skipping")

    print(f"\nDone — files in ./{HEATMAP_DIR}/")


# ── Load all files ────────────────────────────────────────────────────────────

def load_all_heatmap_files(heatmap_dir=HEATMAP_DIR):
    files = [f for f in os.listdir(heatmap_dir) if f.endswith('.csv')]
    if not files:
        print("No files found")
        return pd.DataFrame()
    dfs = [pd.read_csv(os.path.join(heatmap_dir, f)) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    print(f"Loaded {len(files)} files — {len(df)} total rows")
    return df
