import yaml
import scraping_script as scs
import itertools
import os
import ssl
import pandas as pd
import importlib
import time
from datetime import date, timedelta, datetime



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

with open("code_run_config_default.yaml", 'r') as stream:
    cr_config = yaml.safe_load(stream)


schedules = list()
for key in cr_config['data_run']:
    temp_dict = cr_config['data_run'][key]
    league_tag = temp_dict['league_tag']
    season = temp_dict['season']
    print(league_tag, season)
    league = info[league_tag]
    raw_schedule = scs.scrape_schedule(league, season)
    schedule = scs.clean_schedule(raw_schedule, data_config, league, season)
    schedules.append(schedule)
    time.sleep(10)

full_schedule = pd.concat(schedules, ignore_index=True)
if  cr_config['use_start_date']: 
    full_schedule = full_schedule[full_schedule.match_date >= cr_config['start_date']]

if cr_config['use_end_date']:
    full_schedule = full_schedule[full_schedule.match_date <= cr_config['end_date']]

competitions = scs.build_competitions_df(info)
scs.upsert_df(competitions, 'dim_competitions', db_config)

scs.upsert_df(full_schedule, 'dim_matches', db_config)
scs.upsert_df(full_schedule, 'dim_squads', db_config)

teams = scs.build_team_schedules(full_schedule)
scs.upsert_df(teams, 'dim_team_matches', db_config)



scs.scrape_from_schedule(full_schedule, scraping_config)

ids = list(full_schedule.id)
categories = ['summary', 'passing', 'possession', 'defense','shots', 'possession', 'passing_types', 'keeper']
combos = list(itertools.product(ids, categories))
for j in combos:
    print(j)
    match_id = j[0]
    cat = j[1]
    if cat != 'shots':
        try:
            raw_home_file = 'raw_data/match_reports/{}/home_team_{}_{}.pkl'.format(cat, cat, match_id)
            raw_away_file = 'raw_data/match_reports/{}/away_team_{}_{}.pkl'.format(cat, cat, match_id)
            home_file = 'data/match_reports/{}/home_team_{}_{}.pkl'.format(cat, cat, match_id)
            away_file = 'data/match_reports/{}/away_team_{}_{}.pkl'.format(cat, cat, match_id)
            scs.clean_match_report(raw_home_file, cat, data_config)
            scs.clean_match_report(raw_away_file, cat, data_config)
            home_df = pd.read_pickle(home_file)
            away_df = pd.read_pickle(away_file)
            table = 'f_player_match_{}'.format(cat)
            idf = pd.concat([away_df, home_df], ignore_index=True)
        except Exception as e:
            print(e)
            idf = None
    else:
        try:
            raw_file = 'raw_data/shots/all_shots_{}.pkl'.format(match_id)
            file = 'data/match_reports/shots/all_shots_{}.pkl'.format(match_id)
            scs.clean_match_report(raw_file, cat, data_config)
            idf = pd.read_pickle(file)
            table = 'f_shots'
        except Exception as e:
            print(e)
            idf = None
        
    

    if cat == 'summary' and idf is not None:
        scs.upsert_df(idf, 'dim_player_appearances', db_config)
        scs.upsert_df(idf, 'dim_players', db_config)

    
    if idf is not None:
        scs.upsert_df(idf, table, db_config)

et = datetime.now()
print(len(ids))
print(et-st)