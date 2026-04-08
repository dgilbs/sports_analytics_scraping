import os
import time
import random
import requests
import pandas as pd
from datetime import date

# ── Config ───────────────────────────────────────────────────────────────────

OUTPUT_DIR = 'nwsl_match_stats'
os.makedirs(OUTPUT_DIR, exist_ok=True)

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
    """GET with retry logic and exponential backoff on rate limits."""
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
    """seasons = {'2025': 71412, '2026': 88711}"""
    rows = []
    for year, season_id in seasons.items():
        events = get_season_events(1690, season_id)
        for e in events:
            rows.append({
                'event_id':          e['id'],
                'season':            year,
                'round':             e.get('roundInfo', {}).get('round'),
                'home_team':         e['homeTeam']['name'],
                'away_team':         e['awayTeam']['name'],
                'home_score':        e['homeScore'].get('current'),
                'away_score':        e['awayScore'].get('current'),
                'winner_code':       e.get('winnerCode'),
                'status':            e['status']['description'],
                'start_timestamp':   e['startTimestamp'],
                'has_player_stats':  e.get('hasEventPlayerStatistics', False),
            })
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['start_timestamp'], unit='s').dt.date
    df = df.sort_values('date').reset_index(drop=True)
    return df


# ── Stats extraction ──────────────────────────────────────────────────────────

def extract_stats(stats):
    return {
        'minutes_played':               stats.get('minutesPlayed'),
        'rating':                        stats.get('rating'),
        'rating_alternative':            stats.get('ratingVersions', {}).get('alternative'),
        'goals':                         stats.get('goals', 0),
        'assists':                       stats.get('goalAssist', 0),
        'key_passes':                    stats.get('keyPass', 0),
        'total_shots':                   stats.get('totalShots', 0),
        'shots_on_target':               stats.get('onTargetScoringAttempt', 0),
        'shots_off_target':              stats.get('shotOffTarget', 0),
        'shots_blocked':                 stats.get('blockedScoringAttempt', 0),
        'big_chance_missed':             stats.get('bigChanceMissed', 0),
        'total_offside':                 stats.get('totalOffside', 0),
        'total_pass':                    stats.get('totalPass'),
        'accurate_pass':                 stats.get('accuratePass'),
        'total_long_balls':              stats.get('totalLongBalls'),
        'accurate_long_balls':           stats.get('accurateLongBalls'),
        'total_cross':                   stats.get('totalCross', 0),
        'accurate_cross':                stats.get('accurateCross', 0),
        'own_half_passes':               stats.get('totalOwnHalfPasses'),
        'accurate_own_half_passes':      stats.get('accurateOwnHalfPasses'),
        'opp_half_passes':               stats.get('totalOppositionHalfPasses'),
        'accurate_opp_half_passes':      stats.get('accurateOppositionHalfPasses'),
        'carries_count':                 stats.get('ballCarriesCount'),
        'carries_distance':              stats.get('totalBallCarriesDistance'),
        'progressive_carries_count':     stats.get('progressiveBallCarriesCount'),
        'progressive_carries_distance':  stats.get('totalProgressiveBallCarriesDistance'),
        'total_progression':             stats.get('totalProgression'),
        'best_carry_progression':        stats.get('bestBallCarryProgression'),
        'touches':                       stats.get('touches'),
        'unsuccessful_touch':            stats.get('unsuccessfulTouch', 0),
        'possession_lost':               stats.get('possessionLostCtrl'),
        'dispossessed':                  stats.get('dispossessed', 0),
        'duel_won':                      stats.get('duelWon', 0),
        'duel_lost':                     stats.get('duelLost', 0),
        'aerial_won':                    stats.get('aerialWon', 0),
        'aerial_lost':                   stats.get('aerialLost', 0),
        'total_contest':                 stats.get('totalContest', 0),
        'won_contest':                   stats.get('wonContest', 0),
        'challenge_lost':                stats.get('challengeLost', 0),
        'total_tackle':                  stats.get('totalTackle', 0),
        'won_tackle':                    stats.get('wonTackle', 0),
        'interception_won':              stats.get('interceptionWon', 0),
        'total_clearance':               stats.get('totalClearance', 0),
        'ball_recovery':                 stats.get('ballRecovery', 0),
        'fouls':                         stats.get('fouls', 0),
        'was_fouled':                    stats.get('wasFouled', 0),
        'shot_value':                    stats.get('shotValueNormalized'),
        'pass_value':                    stats.get('passValueNormalized'),
        'dribble_value':                 stats.get('dribbleValueNormalized'),
        'defensive_value':               stats.get('defensiveValueNormalized'),
    }


def get_match_player_stats_full(row, player_sleep=0.3, match_sleep=0.5):
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

            stat_resp = safe_get(
                f"https://api.sofascore.com/api/v1/event/{event_id}/player/{player_id}/statistics",
                headers=headers
            )
            time.sleep(player_sleep + random.uniform(0, 0.1))

            stats = {}
            if stat_resp and stat_resp.status_code == 200:
                stats = stat_resp.json().get('statistics', {})

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
            }
            row_data.update(extract_stats(stats))
            rows.append(row_data)

    time.sleep(match_sleep)
    return rows


# ── Fetch and write per-match files ───────────────────────────────────────────

def fetch_stats_for_dates(df_matches, start_date, end_date, statuses=('Ended', 'AET', 'AP')):
    mask = (
        (df_matches['date'] >= pd.to_datetime(start_date).date()) &
        (df_matches['date'] <= pd.to_datetime(end_date).date()) &
        (df_matches['status'].isin(statuses))
    )
    subset = df_matches[mask].reset_index(drop=True)
    print(f"Fetching stats for {len(subset)} matches ({start_date} → {end_date})\n")

    for i, row in subset.iterrows():
        event_id  = row['event_id']
        home_team = row['home_team'].replace('/', '-')
        away_team = row['away_team'].replace('/', '-')
        date      = row['date']

        filename = f"{OUTPUT_DIR}/{event_id}.csv"

        if os.path.exists(filename):
            print(f"[{i+1}/{len(subset)}] Skipping — already exists: {filename}")
            continue

        print(f"[{i+1}/{len(subset)}] {row['home_team']} vs {row['away_team']} ({date})")
        rows = get_match_player_stats_full(row)

        if rows:
            pd.DataFrame(rows).to_csv(filename, index=False)
            print(f"  Saved {len(rows)} rows → {filename}")
        else:
            print(f"  No data returned, skipping file write")

    print(f"\nDone — files in ./{OUTPUT_DIR}/")


# ── Load all files into one dataframe ────────────────────────────────────────

def load_all_match_files(output_dir=OUTPUT_DIR):
    files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
    if not files:
        print("No files found")
        return pd.DataFrame()
    dfs = [pd.read_csv(os.path.join(output_dir, f)) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    print(f"Loaded {len(files)} files — {len(df)} total rows")
    return df