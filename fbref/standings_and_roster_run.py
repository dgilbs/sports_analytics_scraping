import pandas as pd
import yaml
import scraping_script as scs
import time
import logging
import datetime
import warnings

# timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
# log_filename = f"log_{timestamp}.txt"

# # Configure logging
# logging.basicConfig(
#     filename=log_filename,
#     level=logging.INFO,  # Change to DEBUG, ERROR, etc. as needed
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )

warnings.filter('ignore')

with open("data_config.yaml", 'r') as stream:
    data_config = yaml.safe_load(stream)
    
with open("db_config.yaml", 'r') as stream:
    db_config = yaml.safe_load(stream)

with open("leagues.yaml", 'r') as stream:
    leagues = yaml.safe_load(stream)

with open("roster_scraping_config.yaml") as stream:
    roster_config = yaml.safe_load(stream)


combos = list()
for i in roster_config:
    seasons = roster_config[i]
    for k in seasons:
        row = [i, k]
        combos.append(row)

conn_string =  "postgresql://danielgilberg:password@localhost:5432/projects"
for i in combos:
    info = leagues[i[0]]
    folder = info['folder']
    season = i[1]
    standings = scs.scrape_standings(info, season)
    ids = list(standings.squad_id)
    scs.scrape_rosters_from_standings_df(standings, data_config, info)
    for j in ids:
        fp = 'raw_data/rosters/{}/{}_{}_roster.pkl'.format(folder, season, j)
        print(fp)
        roster = scs.clean_rosters(fp, data_config, info)
        scs.upsert_df(roster, 'dim_squad_rosters', conn_string, ['id'], db_config)
    time.sleep(10)