import pandas as pd
import os
import yaml
import time
import requests
import numpy as np
import sqlite3
import ssl
import warnings
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
from datetime import datetime
from bs4 import BeautifulSoup

def scrape_schedule(config, info, season):
    league_name = info['name']
    league_tag = info['url_tag']
    url = "https://www.basketball-reference.com/{}/years/{}_games.html".format(league_tag, str(season))
    df = pd.read_html(url, extract_links='body')[0]
    dir_path = 'raw_data/schedules/{}'.format(league_name)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_name = '{}_schedule.pkl'.format(season)
    rfp = os.path.join(dir_path, file_name)
    df.to_pickle(rfp)
    if 'Notes' in df.columns:
        df.columns = config['schedule_rename_columns']
    else:
        df.columns = config['schedule_rename_columns'][:-1]
    link_cols = config['schedule_link_columns']
    non_link_cols = [i for i in df.columns if i not in link_cols]
    for i in link_cols:
        new_col = i + '_link'
        df[new_col] = df.apply(lambda row: row[i][1], axis=1)
        df[i] = df.apply(lambda row: row[i][0], axis=1)

    for j in non_link_cols:
        df[j] = df.apply(lambda row: row[j][0], axis=1)
    first_playoff_index = df[df['home_team'] == 'Playoffs'].index
    
    df = df[~pd.isnull(df.away_team_link)]
    if not first_playoff_index.empty:
        first_playoff_index = first_playoff_index[0]
        # Create the 'is_playoffs' column based on the index
        df['is_playoffs'] = df.index > first_playoff_index
    else:
        # If 'Playoffs' is not found, set all to False
        df['is_playoffs'] = False
    if 'notes' in df.columns:
        df['is_commissioners_cup'] = df.notes.str.contains("Commissioner's Cup Game")
    else:
        df['is_commissioners_cup'] = False
    df['away_team_id'] = df.apply(lambda row: row['away_team_link'].split('/')[-2], axis=1)
    df['home_team_id'] = df.apply(lambda row: row['home_team_link'].split('/')[-2], axis=1)
    df['game_id'] = df.apply(lambda row: row['box_score_link'].split('.')[0].split('/')[-1], axis=1)
    df['game_date'] = df.apply(lambda row: datetime.strptime(row['game_date'], "%a, %b %d, %Y").strftime("%Y-%m-%d"), axis=1)

    dir_path = 'data/schedules/{}'.format(league_name)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_name = '{}_schedule.pkl'.format(season)
    fp = os.path.join(dir_path, file_name)
    df.to_pickle(fp)
    return df


def scrape_multiple_season_schedules(config, info, seasons):
    for season in seasons:
        scrape_schedule(config, wnba, season)
        time.sleep(10)

def extract_table_ids(url):
    if 'https://basketball-reference.com' not in url:
        url = 'https://basketball-reference.com' + url
    response = requests.get(url, verify=False)
    html_content = response.text
    
    # Parse with BeautifulSoup to find tables with IDs
    soup = BeautifulSoup(html_content, 'html.parser')
    tables_with_ids = []
    
    # Loop through each table, extract the ID and data
    for table in soup.find_all('table'):
        table_id = table.get('id')
        tables_with_ids.append(table_id)

    return tables_with_ids

def scrape_box_score(row, info):
    url = 'https://basketball-reference.com' + row['box_score_link']
    league_name = info['name']
    basic_dir_path = 'raw_data/box_scores/{}/basic'.format(league_name)
    advanced_dir_path = 'raw_data/box_scores/{}/advanced'.format(league_name)
    if not os.path.exists(basic_dir_path):
        os.makedirs(basic_dir_path)

    if not os.path.exists(advanced_dir_path):
        os.makedirs(advanced_dir_path)
    table_ids = extract_table_ids(url)
    arr = pd.read_html(url, extract_links='body')
    final_dict = {}
    for index, tid in enumerate(table_ids):
        if tid is not None and ('-q' in tid or 'ot' in tid):
            temp_df = arr[index]
            final_dict[tid] = temp_df
            team = tid.split('-')[1]
            temp_df['team_id'] = team
            temp_df['game_id'] = row['game_id']
            quarter = tid.split('-')[2].upper()
            temp_df['game_quarter'] = quarter
            dir_path = basic_dir_path
            file_name = row['game_id'] + '_' + tid.replace('-', '_') + '.pkl'
        elif tid is not None and 'advanced' in tid:
            temp_df = arr[index]
            final_dict[tid] = temp_df
            team = tid.split('-')[0]
            temp_df['team_id'] = team
            temp_df['game_id'] = row['game_id']
            dir_path = advanced_dir_path
            file_name = row['game_id'] + '_' + tid.replace('-', '_') + '.pkl'
        else:
            continue

        
        fp = os.path.join(dir_path, file_name)
        temp_df.to_pickle(fp)


def scrape_roster(row, info, home=True):
    if home:
        url = row['home_team_link']
        team_id = row['home_team_id']
    else:
        url = row['away_team_link']
        team_id = row['away_team_id']
    league = info['name']
    season = url.split('/')[-1].split('.')[0]
    if 'https://basketball-reference.com' not in url:
        url = 'https://basketball-reference.com' + url
    attrs={'id': 'roster'}
    df = pd.read_html(url, attrs=attrs, extract_links='body')[0]
    df['season'] = season
    df['team_id'] = team_id
    dir_path = 'raw_data/rosters/{}'.format(league)
    file_name = '{}_{}_roster.pkl'.format(team_id, season)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    fp = os.path.join(dir_path, file_name)
    df.to_pickle(fp)
    return df


def clean_roster(fp, info, config):
    df = pd.read_pickle(fp)
    dir, file = os.path.split(fp)
    df.columns = [i.lower().replace(' ', '_') for i in df.columns]
    rename_cols = config['roster_rename_columns']
    df = df.rename(columns=rename_cols)
    link_cols = config['roster_link_columns']
    non_link_cols = [i for i in df.columns if i not in link_cols]
    
    for i in link_cols:
        new_col = i + '_link'
        df[new_col] = df.apply(lambda row: row[i][1], axis=1)
        df[i] = df.apply(lambda row: row[i][0], axis=1)
    for j in non_link_cols:
        df[j] = df.apply(lambda row: row[j][0], axis=1)

    #df['college_grad'] = df.apply(lambda row: [i.strip() for i in row['colleges'] if i != ''], axis=1)
    df['last_college'] = df.apply(lambda row: row['colleges'].split(', ')[-1] if row['colleges'] != '' else None, axis=1)
    team_id = fp.split('/')[-1].split('_')[0]
    df['team_id'] = team_id
    df['player_id'] = df.apply(lambda row: row['player_link'].split('/')[-1].split('.')[0], axis=1)
    df['season'] = file.split('_')[1]
    df['id'] = df['player_id'] + '-' + df['team_id'] + '-' + df['season']
    df['height_inches'] = df.apply(lambda row: height_str_to_inches(row['height']), axis=1)
    dir_path = 'data/rosters/{}/'.format(info['name'])
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    nfp = os.path.join(dir_path, file)
    df.to_pickle(nfp)
    return df

def scrape_box_scores_from_schedule(schedule, info):
    for index, i in schedule.iterrows():
        print(index, i['game_date'], i['game_id'])
        try:
            scrape_box_score(i, info)
            time.sleep(10)
        except Exception as e:
            print(e)
            time.sleep(10)

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


def extract_player_id(player_link_str):
    try:
        value = player_link_str.split('/')[-1].split('.')[0]
    except:
        value = None
    return value

def clean_box_score(file_path, config, info, del_raw=False):
    if 'basic' in file_path:
        basic = True
    else:
        basic = False
    league_name = info['name']
    df = pd.read_pickle(file_path)
    cols = list()
    for col in df.columns:
        if basic and col[0] == 'Basic Box Score Stats':
            new_col = col[1].lower()
        elif basic:
            new_col = col[0].lower()
        elif not basic and col[0] == 'Advanced Box Score Stats': 
            new_col = col[1].lower()
        else: 
            new_col = col[0].lower()
        cols.append(new_col)
    df.columns = cols
    if basic:
        rename_columns = config['basic_box_score_rename_columns']
        link_cols = config['basic_box_score_link_columns']
        non_link_cols = config['basic_box_score_non_link_columns']
    else:
        rename_columns = config['advanced_box_score_rename_columns']
        link_cols = config['advanced_box_score_link_columns']
        non_link_cols = config['advanced_box_score_non_link_columns']
        

    df = df.rename(columns=rename_columns)
    
    df = df[~df.player.isin(['Totals'])]
    for i in link_cols:
        new_col = i + '_link'
        df[new_col] = df.apply(lambda row: row[i][1], axis=1)
        df[i] = df.apply(lambda row: row[i][0], axis=1)
    for j in non_link_cols:
        df[j] = df.apply(lambda row: row[j][0], axis=1)

    dir_path = 'data/box_scores/{}'.format(league_name)
    file_name = os.path.split(file_path)[-1]
    new_file_path = os.path.join(dir_path, file_name)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    df = df[(df.player != 'Reserves') & (df.minutes_played != '')]
    df = cast_dtypes(df, config['box_score_dtypes'])
    df['minutes_played_str'] = '00:' + df.minutes_played
    df['minutes_played_time'] = pd.to_timedelta(df.minutes_played_str)
    df['minutes_played_int'] = df.apply(lambda row: round(row['minutes_played_time'].total_seconds()/60, 4), axis=1)
    df['league'] = info['name']
    df['player_id'] = df.apply(lambda row: extract_player_id(row['player_link']), axis=1)
    if not basic: 
        df['id'] = df.apply(lambda row: row['player_id'] + '-' + row['game_id'] + '-' + 'adv', axis=1)
    else:
        df['id'] = df.apply(lambda row: row['player_id'] + '-' + row['game_id'] + '-' + row['game_quarter'], axis=1)
    df.to_pickle(new_file_path)
    return df

def upsert_df_sqlite(df, table_name, db_config):
    info = db_config[table_name]
    cols = info['df_cols']
    idf = df[cols]
    idf.columns = info['rename_cols']
    for col in idf.columns:
        idf.loc[:, col] = idf[col].fillna(0)
    idf = idf.drop_duplicates(subset=info['key'])
    conn = sqlite3.connect('basketball.db')
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

def height_str_to_inches(height_str):
    """Converts a height string in the format 'ft'in"' to inches."""
    if height_str == '':
        return None
    feet, inches = height_str.split("-")
    feet = int(feet)
    inches = int(inches.replace('"', ''))

    total_inches = feet * 12 + inches
    return total_inches


def build_team_schedules(schedule, info, config):
    rows = list()
    for index, row in schedule.iterrows():
        away_row_id = row['game_id'] + '-' + row['away_team_id']
        home_row_id = row['game_id'] + '-' + row['home_team_id']
        game_date = row['game_date']
        away_team_id = row['away_team_id']
        home_team_id = row['home_team_id']
        away_team_pts = row['away_pts']
        home_team_pts = row['home_pts']
        is_playoffs = row['is_playoffs']
        is_commissioners_cup = row['is_commissioners_cup']
        season = row['season']
        game_id = row['game_id']
        away_row = [away_row_id, game_date, away_team_id, home_team_id, away_team_pts, home_team_pts, is_playoffs, is_commissioners_cup, season, 'Away', game_id]
        home_row = [home_row_id, game_date, home_team_id, away_team_id, home_team_pts, away_team_pts, is_playoffs, is_commissioners_cup,season, 'Home', game_id]
        rows.append(home_row)
        rows.append(away_row)
    final = pd.DataFrame(rows, columns=config['team_schedule_columns'])
    final['margin'] = final.team_pts.astype(int) - final.opponent_pts.astype(int)
    final['game_win'] = final.apply(lambda row: int(row['team_pts']) > int(row['opponent_pts']), axis=1)
    final['league'] = info['name']
    
    return final
    

def upsert_df(df, table_name, conn_string, unique_columns, db_config, schema='basketball', dedupe=False):
    """
    Upserts a pandas DataFrame to a PostgreSQL table.
    
    Parameters:
        df (pd.DataFrame): The DataFrame to upsert.
        table_name (str): The name of the target table.
        conn_string (str): PostgreSQL connection string.
        unique_columns (list): List of column names that uniquely identify a row.
    """
    config = db_config[table_name]
    df_cols = config['df_cols']
    rename_cols = config['rename_cols']
    df = df[df_cols]
    df.columns = rename_cols
    if 'numeric_cols' in config:
        num_cols = config['numeric_cols']
        for col in num_cols:
            df[col] = pd.to_numeric(df.loc[:, col], errors='coerce')
            df[col] = df.loc[:, col].fillna(0)
            #df[col] = df[col].replace({pd.NA: 0, np.nan: 0})

    if dedupe:
        df = df.drop_duplicates(subset=unique_columns, ignore_index=True)
    table_id = '.'.join([schema, table_name])
    df = df.map(lambda x: x.item() if isinstance(x, (pd.Int64Dtype, pd.Float64Dtype, pd.BooleanDtype)) else x)
    # Create SQLAlchemy engine
    engine = create_engine(conn_string)
    with engine.connect() as conn:
        with conn.begin():
            # Ensure column names are valid SQL identifiers
            df.columns = [col.lower() for col in df.columns]
            
            # Get column names
            columns = list(df.columns)
            
            # Generate SQL placeholders
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)
            updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in unique_columns])
            
            # Convert DataFrame to list of tuples
            data = [tuple(row) for row in df.itertuples(index=False, name=None)]
            
            # Generate UPSERT query
            upsert_query = f'''
                INSERT INTO {table_id} ({columns_str})
                VALUES {placeholders}
                ON CONFLICT ({', '.join(unique_columns)})
                DO UPDATE SET {updates};
            '''
            
            # Use psycopg2 to execute query efficiently
            with conn.connection.cursor() as cursor:
                execute_values(cursor, upsert_query.replace(placeholders, '%s'), data)
