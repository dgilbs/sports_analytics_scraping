import yaml
import scraping_script as scs
import itertools
import os
import ssl
import pandas as pd
import importlib

from datetime import datetime, timedelta


st = datetime.now()

importlib.reload(scs)


ssl._create_default_https_context = ssl._create_unverified_context

with open("data_config.yaml", 'r') as stream:
    data_config = yaml.safe_load(stream)
    
with open("db_config.yaml", 'r') as stream:
    db_config = yaml.safe_load(stream)
    
with open("scraping_config.yaml", 'r') as stream:
    scraping_config = yaml.safe_load(stream)
    
with open("scraping_config.yaml", 'r') as stream:
    scraping_config = yaml.safe_load(stream)
    
with open("config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

with open("leagues.yaml", 'r') as stream:
    info = yaml.safe_load(stream)


league_name = 'Frauen-Bundesliga'
season = '2022-2023'
league = info[league_name]

raw_schedule = scs.scrape_schedule(league, season)
schedule = scs.clean_schedule(raw_schedule, data_config, league, season)

competitions = scs.build_competitions_df(info)
scs.upsert_df(competitions, 'dim_competitions', db_config)

scs.upsert_df(schedule, 'dim_matches', db_config)
scs.upsert_df(schedule, 'dim_squads', db_config)

teams = scs.build_team_schedules(schedule)
scs.upsert_df(teams, 'dim_team_matches', db_config)

schedule = schedule.iloc[110:]

scs.scrape_from_schedule(schedule, scraping_config)

ids = list(schedule.id)
categories = ['summary', 'passing', 'possession']
combos = list(itertools.product(ids, categories))
for j in combos:
    match_id = j[0]
    cat = j[1]
    raw_home_file = 'raw_data/match_reports/{}/home_team_{}_{}.pkl'.format(cat, cat, match_id)
    raw_away_file = 'raw_data/match_reports/{}/away_team_{}_{}.pkl'.format(cat, cat, match_id)
    scs.clean_match_report(raw_home_file, cat, data_config)
    scs.clean_match_report(raw_away_file, cat, data_config)
    home_file = 'data/match_reports/{}/home_team_{}_{}.pkl'.format(cat, cat, match_id)
    away_file = 'data/match_reports/{}/away_team_{}_{}.pkl'.format(cat, cat, match_id)
    home_df = pd.read_pickle(home_file)
    away_df = pd.read_pickle(away_file)
    if cat == 'summary':
        scs.upsert_df(home_df, 'dim_player_appearances', db_config)
        scs.upsert_df(away_df, 'dim_player_appearances', db_config)
        scs.upsert_df(home_df, 'dim_players', db_config)
        scs.upsert_df(away_df, 'dim_players', db_config)
    table = 'f_player_match_{}'.format(cat)
    idf = pd.concat([away_df, home_df], ignore_index=True)
    scs.upsert_df(idf, table, db_config)

et = datetime.now()
print(et-st)