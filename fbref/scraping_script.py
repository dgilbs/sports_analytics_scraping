import pandas as pd
import os
import yaml
import time
import numpy as np
import urllib.request
import ssl
import itertools
import json
import itertools
import sqlite3
from datetime import datetime, date, timedelta
# from google.oauth2 import service_account
# from pandas_gbq import to_gbq
from urllib.request import urlopen
# from google.cloud import storage, bigquery


def all_files_in_subdirectories(dir_path, key_term=None):
    """
    a quick an easy way to list the full path of all files in subdirectories

    Args:
        dir_path(str): relative path you are looking at

    returns:
        arr(list): list of all full relative paths in that folder
    """
    #initalize a list
    arr = list()
    #walk through entire file path and append full relative path of each
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            arr.append(os.path.join(root, file))
    #filters for a key term
    if key_term:
        arr = [i for i in arr if key_term in i]
    return arr


def build_dataframe_from_subdirectory(dir_path, key_term=None):
    """
    Takes files in a given file path and builds a dataframe

    Args:
        dir_path(str): relative path to folder
        key_term(str): any key terms in file names

    Returns:
        df(DataFrame): data in folder
    """
    #gets all the files
    files = all_files_in_subdirectories(dir_path, key_term=key_term)
    #concats taht list into a dataframe
    df = pd.concat([pd.read_pickle(i) for i in files], ignore_index=True)
    return df


def cast_dtypes(df, datatypes):
    """
    Casts datatypes to columns in a database

    Args:
        df(DataFrame): DataFrame to assign dtypes to
        dtypes(dict): dict of columns and their corresponding datatypes

    Returns:
        new_df(DataFrame): dataframe with reset datatypes
    """
    new_df = df.copy()
    arr = list(datatypes.keys())
    for i in arr:
        if i in df.columns:
            new_df[i] = new_df[i].fillna(np.nan)
            new_df[i] = new_df[i].replace('', np.nan)
#             if df[i].dtype == 'object':
#                 print(i)
#                 new_df[i] = new_df[i].str.replace(',', '')
            new_df[i] = new_df[i].astype(datatypes[i])
    return new_df

def scrape_schedule(comp_dict, season):
    comp_id = comp_dict['league_id']
    comp_tag = comp_dict['league_table_tag']
    league = comp_dict['name']
    folder = comp_dict['folder']
    url = 'https://fbref.com/en/comps/{}/{}/schedule/{}-{}'.format(comp_id, season, season, comp_tag)
    sched_id = 'sched_{}_{}_1'.format(season, comp_id)
    attrs = {'id': sched_id}
    arr = pd.read_html(url, extract_links='body', attrs=attrs)
    check_cols = ['Referee', 'Venue', 'Match Report']
    for i in arr:
        cols = i.columns
        if all(item in cols for item in check_cols):
            folder = 'raw_data/schedules/{}'.format(folder)
            if not os.path.exists(folder):
                os.makedirs(folder)
            fn = '{}_{}_schedule.pkl'.format(league, season)
            fp = os.path.join(folder, fn)
            i.to_pickle(fp)
    return i


def clean_schedule(df, config, league_info, season_str):
    competition_id = league_info['league_id']
    df.columns = [i.lower().replace(' ', '_') for i in df.columns]
    df = df.rename(columns=config['schedule_rename_columns'])
    link_cols = config['schedule_link_columns']
    non_link_cols = [i for i in df.columns if i not in link_cols]
    for i in link_cols:
        new_col = i + '_link'
        df[new_col] = df.apply(lambda row: row[i][1], axis=1)
        df[i] = df.apply(lambda row: row[i][0], axis=1)
    for j in non_link_cols:
        df[j] = df.apply(lambda row: row[j][0], axis=1)
    #create new columns based on other values and info values
    df = df[(df.day_of_week != 'Day') & (df.match_report == 'Match Report') & (df.score.str.contains('–'))]
    
    df['attendance'] = df.attendance.str.replace(',', '')
    df['home_team_id'] = df.apply(lambda row: row['home_team_link'].split('/')[3], axis=1)
    df['away_team_id'] = df.apply(lambda row: row['away_team_link'].split('/')[3], axis=1)
    df['id'] = df.apply(lambda row: row['match_report_link'].split('/')[-2], axis=1)
    df['competition_id'] = competition_id
    df['home_goals'] = df.apply(lambda row: row['score'].split('–')[0], axis=1)
    df['away_goals'] = df.apply(lambda row: row['score'].split('–')[1], axis=1)
    df['match_date'] = pd.to_datetime(df.match_date).dt.date
    df['season'] = season_str
    folder = 'data/schedules/{}'.format(league_info['folder'])
    if not os.path.exists(folder):
        os.makedirs(folder)
    file_name = '{}_schedule.pkl'.format(season_str)
    fp = os.path.join(folder, file_name)
    df.to_pickle(fp)
    return df


def scrape_match_reports(row, scraping_config):
    url = 'https://www.fbref.com' + row['match_report_link']
    arr = pd.read_html(url, extract_links='body')
    folders = scraping_config['match_report_folder']
    cols = scraping_config['match_report_path']
    match_id = row['id']
    for index, i in enumerate(arr):
        fp = cols.get(index)
        folder = folders.get(index)
        if fp is not None and folder is not None:
            if 'home' in fp:
                i['team_id'] = row['home_team_id']
            else:
                i['team_id'] = row['away_team_id']
            file_path = fp.format(folder, match_id)
            dir_path, name = os.path.split(file_path)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            i.to_pickle(file_path)


def clean_match_report(file_path, keyword, config, del_raw=False):
    df = pd.read_pickle(file_path)
    columns = list()
    for i in df.columns:
        cat = i[0].replace(' ', '_')
        col = i[1].replace(' ', '_')
        if 'Unnamed' in cat and cat != 'team_id':
            new_col = col.lower().strip()
        elif cat != 'team_id':
            new_col = cat.lower().strip() + '_' + col.lower().strip()
        else: 
            new_col = cat.strip()
        columns.append(new_col)
        
    df.columns = columns
    rename_col_str = 'match_report_{}_rename_columns'.format(keyword)
    if keyword != 'shots':
        df = df.rename(columns=config[rename_col_str])
        df = df.dropna(subset=['shirtnumber'])
    link_cols = [i for i in df.columns if i in config['match_report_link_columns']]
    non_link_cols = [i for i in df.columns if i not in link_cols and i != 'team_id']
    for j in link_cols:
        new_col = j + '_link'
        df[new_col] = df.apply(lambda row: row[j][1], axis=1)
        df[j] = df.apply(lambda row: row[j][0], axis=1)

    for k in non_link_cols:
        df[k] = df.apply(lambda row: row[k][0], axis=1)
        
    dtype_str = 'match_report_{}_dtypes'.format(keyword)
    df = cast_dtypes(df, config[dtype_str])
    name = os.path.split(file_path)[1]
    match_id = name.split('.pkl')[0].split('_')[-1].split('(')[0]
    if keyword == 'shots':
        df = df.dropna(subset=['xg'])
    df['player_id'] = df.apply(lambda row: row['player_link'].split('/')[-2], axis=1)
    df['match_id'] = match_id
    if keyword != 'shots':
        df['id'] = df.apply(lambda row: "-".join([row['player_id'], row['team_id'], row['match_id']]), axis=1)
    else:
        df['row_number'] = df.index + 1
        df['id'] = df.apply(lambda row: "-".join([row['player_id'], row['match_id'], str(row['row_number'])]), axis=1)
        df['sca_1_player_id'] = df.apply(lambda row: extract_id(row['sca_1_player_link'], -2), axis=1)
        df['sca_2_player_id'] = df.apply(lambda row: extract_id(row['sca_2_player_link'], -2), axis=1)
        
        df = df.drop(['row_number'], axis=1)
        df['team_id'] = df.apply(lambda row: extract_squad_id_for_shots(row['squad_link']), axis=1)
        df['minute'] = df.apply(lambda row: str(row['minute']).split('+')[0], axis=1)
    dir = 'data/match_reports/{}'.format(keyword)
    if not os.path.exists(dir):
        os.makedirs(dir)

    

    new_path = os.path.join(dir, name)
    df.to_pickle(new_path)
    if del_raw:
        os.remove(file_path)
    return df


def scrape_from_schedule(schedule_df, scraping_config, start_date=None, end_date=None):
    if start_date is not None: 
        schedule_df = schedule_df[schedule_df.match_date >= start_date]


    if end_date is not None:
        schedule_df = schedule_df[schedule_df.match_date <= end_date]

    print('scraping {} matches'.format(len(schedule_df)))

    
    for index,i in schedule_df.reset_index(drop=True).iterrows():
        print(index, i['match_date'], i['home_team'], i['away_team'])
        scrape_match_reports(i, scraping_config)
        time.sleep(10)


def create_table_framework(bq_config_dict, creds_path, only_staging=True):
    with open(creds_path) as source:
        info = json.load(source)
    
    storage_credentials = service_account.Credentials.from_service_account_info(info)
        
    storage_client = storage.Client(project=project_id, credentials=storage_credentials)
    table_cols = bq_config_dict['table_columns']
    key_column = bq_config_dict['primary_key']
    staging_table_name = bq_config_dict['staging_table_name']
    table = bq_config_dict['table_name']
    fp = bq_config_dict['file_path']
    project = bq_config_dict['project_id']
    fn = os.listdir(fp)[0]
    fpath = os.path.join(fp, fn)
    df = pd.read_pickle(fpath)
    idf = df[table_cols]
    idf.to_gbq(staging_table_name, project_id=project, if_exists='replace', credentials=storage_credentials, progress_bar=False)
    if not only_staging:
        idf.to_gbq(table, project_id=project, if_exists='replace', credentials=storage_credentials, progress_bar=False)


def clean_all_match_reports_in_folder(file_path, key_word, config, del_raw=False):
    files = [os.path.join(file_path, i) for i in os.listdir(file_path)]
    for file in files:
        clean_match_report(file, key_word, config, del_raw=del_raw)


def upload_to_bq(bq_config_dict, creds_path):
    with open(creds_path) as source:
        info = json.load(source)

    table_cols = bq_config_dict['table_columns']
    key_column = bq_config_dict['primary_key']
    staging_table_name = bq_config_dict['staging_table_name']
    table = bq_config_dict['table_name']
    fp = bq_config_dict['file_path']
    project_id = bq_config_dict['project_id']
    
    storage_credentials = service_account.Credentials.from_service_account_info(info)
        
    storage_client = storage.Client(project=project_id, credentials=storage_credentials)
    

    client = bigquery.Client(project=project_id, credentials=storage_credentials)
    staging_table = client.get_table(staging_table_name)
    staging_columns = [schema_field.name for schema_field in staging_table.schema]
    merge_condition = " AND ".join([f"T.{key_column} = S.{key_column}"])
    update_set_clause = ", ".join([f"T.{col} = S.{col}" for col in staging_columns if col != key_column])
    insert_columns = ", ".join(staging_columns)
    insert_values = ", ".join([f"S.{col}" for col in staging_columns])
    merge_query = f"""
        MERGE `{table}` T
        USING `{staging_table}` S
        ON {merge_condition}
        WHEN MATCHED THEN
          UPDATE SET {update_set_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values})
        """
    files = all_files_in_subdirectories(fp)
    print('inserting {} rows'.format(len(files)))
    try:
        df = build_dataframe_from_subdirectory(fp)
        idf = df[table_cols]
        idf.to_gbq(staging_table_name, project_id=project_id, if_exists='replace', credentials=storage_credentials, progress_bar=False)
        job = client.query(merge_query)
    except Exception as e:
        print(e)


def upsert_df(df, table_name, db_config):
    info = db_config[table_name]
    cols = info['df_cols']
    idf = df[cols]
    idf.columns = info['rename_cols']
    for col in idf.columns:
        idf.loc[:, col] = idf[col].fillna(0)
    idf = idf.drop_duplicates(subset=info['key'])
    conn = sqlite3.connect('soccer.db')
    cursor = conn.cursor()
    columns = ', '.join(idf.columns)
    placeholders = ', '.join(['?'] * len(idf.columns))
    update_columns = ', '.join([f'{col}=excluded.{col}' for col in idf.columns if col != 'id'])
    for index, row in idf.iterrows():
        sql = f'''
        INSERT INTO {table_name} ({columns}) VALUES ({placeholders})
        ON CONFLICT(id) DO UPDATE SET {update_columns}
        '''
        cursor.execute(sql, tuple(row))
        
        
    conn.commit()
    conn.close()
    
    
def build_competitions_df(info):
    rows = list()
    for i in info:
        temp_dict = info[i]
        row = [temp_dict['league_id'], temp_dict['name'], temp_dict['gender']]
        rows.append(row)
    df = pd.DataFrame(rows, columns=['id', 'competition', 'gender'])
    return df

    
def build_team_schedules(schedule_df):
    df_rows = list()
    for index, row in schedule_df.iterrows():
        home_team = row['home_team_id']
        away_team = row['away_team_id']
        season = row['season']
        competition_id = row['competition_id']
        home_goals = row['home_goals']
        away_goals = row['away_goals']
        home_xg = row['home_xg']
        away_xg = row['away_xg']
        match_id = row['id']
        md = row['match_date']
        home_id = row['home_team_id'] + '-' + row['id']
        away_id = row['away_team_id'] + '-' + row['id']
        home_row = [home_id, home_team, away_team, season, competition_id, home_goals, away_goals, home_xg, away_xg,
                   match_id, md, 'Home']
        df_rows.append(home_row)
        away_row = [away_id, away_team, home_team, season, competition_id, away_goals, home_goals, away_xg, home_xg,
                   match_id, md, 'Away']
        df_rows.append(away_row)

    cols = ['id', 'team_id', 'opponent_id', 'season', 'competition_id', 'goals_scored', 'goals_against',
              'xg_for', 'xg_against', 'match_id', 'match_date', 'home_or_away']
    final = pd.DataFrame(df_rows, columns=cols)
    return final


def extract_squad_id_for_shots(squad_link_str):
    arr = squad_link_str.split('/')
    squad_index = arr.index('squads')
    return arr[squad_index+1]


def extract_id(id_str, id_index):
    try:
        id = id_str.split('/')[id_index]
    except:
        id = None
    return id


def scrape_standings(info, season):
    league_id = info['league_id']
    league_name = info['name']
    league_tag = info['league_table_tag']
    folder = info['folder']
    url = 'https://fbref.com/en/comps/{}/{}/{}-{}'.format(league_id, season, season, league_tag)
    df = pd.read_html(url, extract_links='body')[0]
    df.columns = [i.lower() for i in df.columns]
    raw_data_folder = 'raw_data/standings/{}'.format(folder)
    fp = '{}_{}_standings.pkl'.format(season, league_name)
    rfp = os.path.join(raw_data_folder, fp)
    if not os.path.exists(raw_data_folder):
        os.makedirs(raw_data_folder)
    df.to_pickle(rfp)
    
    link_cols = ['squad']
    non_link_cols = [i for i in df.columns if i not in link_cols]
    
    for i in link_cols:
        new_col = i + '_link'
        df[new_col] = df.apply(lambda row: row[i][1], axis=1)
        df[i] = df.apply(lambda row: row[i][0], axis=1)
        
    for j in non_link_cols:
        df[j] = df.apply(lambda row: row[j][0], axis=1)
    df['season'] = season
    df['squad_id'] = df.apply(lambda row: row['squad_link'].split('/')[3], axis=1)
    data_folder = 'data/standings/{}'.format(folder)
    dfp = os.path.join(data_folder, fp)
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
    df.to_pickle(dfp)
    return df


def scrape_rosters_from_standings_row(row, config, info):
    squad_link = row['squad_link']
    season = row['season']
    squad_id = row['squad_id']
    folder = info['folder']
    league = info['name']
    league_id = info['league_id']
    table_id = 'stats_standard_{}'.format(league_id)
    url = 'https://fbref.com' + squad_link
    attrs = {'id': table_id}
    df = pd.read_html(url, extract_links='body', attrs=attrs)[0]
    raw_data_folder = 'raw_data/rosters/{}'.format(folder)
    if not os.path.exists(raw_data_folder):
        os.makedirs(raw_data_folder)
    file_name = '{}_{}_roster.pkl'.format(season, squad_id)
    file_path = os.path.join(raw_data_folder, file_name)
    df.to_pickle(file_path)
    return df

def scrape_rosters_from_standings_df(standings, config, info):
    for index, i in standings.iterrows():
        print(index, i['squad'], i['season'])
        scrape_rosters_from_standings_row(i, config, info)
        time.sleep(10)
        

def clean_rosters(file_path, config, info):
    df = pd.read_pickle(file_path)
    df.columns = [i[1].lower() for i in df.columns]
    use_cols = config['roster_use_cols']
    df = df[use_cols]
    df['player_link'] = df.apply(lambda row: row['player'][1], axis=1)
    df['player'] = df.apply(lambda row: row['player'][0], axis=1)
    df = df[~df.player.isin(['S', 'O'])]
    df['nation'] = df.apply(lambda row: row['nation'][0].split(' ')[-1], axis=1)
    df['pos'] = df.apply(lambda row: ','.join(list(row['pos'])[:-1]), axis=1)
    df['age'] = df.apply(lambda row: row['age'][0], axis=1)
    folder, name = os.path.split(file_path)
    season = name.split('_')[0]
    squad_id = name.split('_')[1]
    df['season'] = season
    df['squad_id'] = squad_id
    df['player_id'] = df.apply(lambda row: row['player_link'].split('/')[-2], axis=1)
    df['id'] = df.apply(lambda row: "-".join([row['player_id'], row['squad_id'], row['season']]), axis=1)
    data_folder = 'data/rosters/{}'.format(info['folder'])
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
    fp = os.path.join(data_folder, name)
    df.to_pickle(fp)
    return df
