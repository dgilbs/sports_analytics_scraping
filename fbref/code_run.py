import yaml
import scraping_script as scs
import itertools
import os
import ssl
import pandas as pd
import importlib
import time
import sys
import warnings
from datetime import date, timedelta, datetime
current_dir = os.getcwd()
parent_dir = os.path.abspath(os.path.join(current_dir, '../base_code/'))
sys.path.append(parent_dir)
import query_db as qdb

conn_string = 'postgresql://neondb_owner:npg_RSU6cfsvr8zy@ep-round-boat-aeeid91z-pooler.c-2.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require'


st = datetime.now()

importlib.reload(scs)

pd.set_option('future.no_silent_downcasting', True)


ssl._create_default_https_context = ssl._create_unverified_context

with open("data_config.yaml", 'r') as stream:
    data_config = yaml.safe_load(stream)
    
with open("db_config.yaml", 'r') as stream:
    db_config = yaml.safe_load(stream)
    
with open("scraping_config.yaml", 'r') as stream:
    scraping_config = yaml.safe_load(stream)
    
    
with open("config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

with open("leagues.yaml", 'r') as stream:
    info = yaml.safe_load(stream)

with open("code_run_config_default.yaml", 'r') as stream:
    cr_config = yaml.safe_load(stream)

if cr_config['filter_leagues']:
    league_list = cr_config['use_leagues']
else:
    league_list = cr_config['data_run'].keys()

schedules = list()
for key in league_list:
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
print(len(full_schedule), cr_config['start_date'])
if  cr_config['use_start_date']: 
    full_schedule = full_schedule[full_schedule.match_date >= cr_config['start_date']]

if cr_config['use_end_date']:
    full_schedule = full_schedule[full_schedule.match_date <= cr_config['end_date']]

    
#conn_string =  "postgresql://danielgilberg:password@localhost:5432/projects"
competitions = scs.build_competitions_df(info)
scs.upsert_df(competitions, 'dim_competitions', conn_string, ['id'], db_config, dedupe=True)
print('competitions done')

scs.upsert_df(full_schedule, 'dim_matches', conn_string, ['id'], db_config, dedupe=True)
print('matches done')
scs.upsert_df(full_schedule, 'dim_squads', conn_string, ['id'], db_config, dedupe=True)
print('squads done')

teams = scs.build_team_schedules(full_schedule)
scs.upsert_df(teams, 'dim_team_matches', conn_string, ['id'], db_config)

#(df, 'f_player_match_passing', conn_string, ['id'], db_config)


scs.scrape_from_schedule(full_schedule, scraping_config)
ids = list(full_schedule.id)
ids = [i for i in ids if i != '6fe8b81d']
categories = ['summary', 'passing', 'possession', 'defense','shots', 'possession', 'passing_types', 'keeper', 'misc']
combos = list(itertools.product(ids, categories))
for j in combos:
    print(j)
    errors = list()
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
            row = ['Cleaning', match_id, cat]
            errors.append(row)
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
        scs.upsert_df(idf, 'dim_player_appearances', conn_string, ['id'], db_config)
        scs.upsert_df(idf, 'dim_players', conn_string, ['id'], db_config)

    if idf is not None:
        try:
            scs.upsert_df(idf, table, conn_string, ['id'], db_config)
        except Exception as e:
            print(e)
            row = ['Database', match_id, cat]
            errors.append(row)

et = datetime.now()
# print(len(ids))
print(et-st)
if len(errors) > 0:
    print('errors found:' )
    for k in errors:
        print(k)
else: 
    print('no errors found')

qdb.backup_all_soccer_tables(conn_string)
print('backup complete')



