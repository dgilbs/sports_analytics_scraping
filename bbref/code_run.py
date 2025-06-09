import bbref_scraping as bbs
import os
import yaml
import pandas as pd
import warnings
import ssl
from datetime import datetime


t1 = datetime.now()

ssl._create_default_https_context = ssl._create_unverified_context

warnings.filterwarnings('ignore')

with open("info.yaml", 'r') as stream:
    info = yaml.safe_load(stream)

with open("config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

with open("db_config.yaml", 'r') as stream:
    db_config = yaml.safe_load(stream)

with open("code_run_config.yaml", 'r') as stream:
    cr_config = yaml.safe_load(stream)

wnba = info['WNBA']

schedule = bbs.scrape_schedule(config, wnba, cr_config['season'])
conn_string =  "postgresql://danielgilberg:password@localhost:5432/projects"

schedule = schedule[schedule.box_score == 'Box Score']
schedule['season'] = cr_config['season']
schedule['league'] = cr_config['league']
schedule['game_date'] = pd.to_datetime(schedule.game_date).dt.date

if cr_config['use_start_date']:
    schedule = schedule[schedule.game_date >= cr_config['start_date']]

if cr_config['use_end_date']:
    schedule = schedule[schedule.game_date <= cr_config['end_date']]

bbs.upsert_df(schedule, 'dim_games', conn_string, ['id'], db_config)

results = bbs.build_team_schedules(schedule, wnba, config)

bbs.upsert_df(results, 'dim_team_results', conn_string, ['id'], db_config)

bbs.scrape_box_scores_from_schedule(schedule, wnba)

basic_folder = 'raw_data/box_scores/WNBA/basic/'
adv_folder = 'raw_data/box_scores/WNBA/advanced/'

basic_clean = 'data/box_scores/WNBA'

gids = list(schedule.game_id.unique())

basic_files = [i for i in os.listdir(basic_folder) if i.split('_')[0] in gids]

advanced_files = [i for i in os.listdir(advanced)]

for file in basic_files:
    fp = os.path.join(basic_folder, file)
    bbs.clean_box_score(fp, config, wnba)
    cfp = os.path.join(basic_clean, file)
    df = pd.read_pickle(cfp)
    bbs.upsert_df(df, 'f_basic_box_score', conn_string, ['id'], db_config)
    

t2 = datetime.now()

print(t2-t1)

qdb.backup_all_basketball_tables(conn_string)
print('backup complete')

#(df, table_name, conn_string, unique_columns, db_config, schema='basketball', dedupe=False)

