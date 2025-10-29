import pandas as pd
import os
import time
import yaml

def scrape_season_summary(season, scraping_config):
    url = 'https://www.sports-reference.com/cbb/seasons/women/{}.html'.format(season)
    dfs = pd.read_html(url, extract_links='body')
    scraping_info = scraping_config['season_summary']
    for i in scraping_info:
        temp_df = dfs[i]
        folder = scraping_info[i]
        if not os.path.exists(folder):
            os.makedirs(folder)
        file_name = folder.split('/')[-1] + '_{}.pkl'.format(season)
        fp = os.path.join(folder, file_name)
        temp_df.to_pickle(fp)
    return True

def clean_season_summary(fp, data_config):
    df = pd.read_pickle(fp)
    # Extract text from column tuples if they are tuples, otherwise use as-is
    df.columns = [i[0].lower().replace(' ', '_') if isinstance(i, tuple) else i.lower().replace(' ', '_') for i in df.columns]    
    link_columns = data_config['season_summary']['link_columns']
    non_link_columns = [i for i in df.columns if i not in link_columns]
    
    for col in link_columns:
        new_col = col + '_link'
        df[new_col] = df.apply(lambda row: row[col][1] if isinstance(row[col], tuple) and len(row[col]) > 1 else None, axis=1)
        df[col] = df.apply(lambda row: row[col][0] if isinstance(row[col], tuple) else row[col], axis=1)
    
    for col in non_link_columns:
        df[col] = df.apply(lambda row: row[col][0] if isinstance(row[col], tuple) else row[col], axis=1)

    year = fp.split('.')[0].split('/')[-1].split('_')[-1]
    
    save_name = '{}.pkl'.format(year)
    save_folder = 'data/season_summary_by_conference'
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    save_path = os.path.join(save_folder, save_name)
    df.to_pickle(save_path)

        
    return df

def scrape_conference_season_summary(season, scraping_config, row):
    url = 'https://www.sports-reference.com' + row['conference_link']
    scraping_info = scraping_config['season_conference_summary']
    dfs = pd.read_html(url, extract_links='body')
    conference = row['conference_link'].split('/')[-3]
    for i in scraping_info:
        temp_df = dfs[i]
        folder = scraping_info[i]
        if not os.path.exists(folder):
            os.makedirs(folder)
        file_name = '{}_{}.pkl'.format(conference, season)
        fp = os.path.join(folder, file_name)
        temp_df.to_pickle(fp)


def scrape_all_season_conference_summaries(summary_df, season, scraping_config):
    for index, row in summary_df.iterrows():
        print(season, row['conference'])
        scrape_conference_season_summary(season, scraping_config, row)
        time.sleep(6)

def clean_conference_standings(fp, data_config):
    df = pd.read_pickle(fp)
    file_name = os.path.split(fp)[1]
    info = file_name.split('.')[0]
    conference = info.split('_')[0].replace('-', '_')
    season = info.split('_')[1]
    df.columns = data_config['season_conference_standings']['column_names']
    link_columns = data_config['season_conference_standings']['link_columns']
    non_link_columns = [i for i in df.columns if i not in link_columns]
    for col in link_columns:
        new_col = col + '_link'
        df[new_col] = df.apply(lambda row: row[col][1] if isinstance(row[col], tuple) and len(row[col]) > 1 else None, axis=1)
        df[col] = df.apply(lambda row: row[col][0] if isinstance(row[col], tuple) else row[col], axis=1)
    
    for col in non_link_columns:
        df[col] = df.apply(lambda row: row[col][0] if isinstance(row[col], tuple) else row[col], axis=1)


    df['conference'] = conference
    df['season'] = season
    save_name = os.path.split(fp)[1]
    save_folder = 'data/season_conference_standings'
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)
    save_path = os.path.join(save_folder, save_name)
    df.to_pickle(save_path)
    return df


def scrape_team_schedule(row, scraping_config):
    scraping_info = scraping_config['team_schedule']
    link = row['school_link']
    arr = link.split('.')[0].split('/')
    tag = arr[3]
    season = arr[-1]
    team = row['school'].replace(' ', '-')
    url = "https://www.sports-reference.com/cbb/schools/{}/women/{}-schedule.html".format(tag, season)
    dfs = pd.read_html(url, extract_links='body')
    for i in scraping_info:
        temp_df = dfs[i]
        folder = scraping_info[i]
        if not os.path.exists(folder):
            os.makedirs(folder)
        file_name = '{}_{}.pkl'.format(team, season)
        fp = os.path.join(folder, file_name)
        temp_df.to_pickle(fp)
        time.sleep(6)

def clean_team_schedule(fp, data_config):
    df = pd.read_pickle(fp)
    info = data_config['team_schedule']
    df.columns = info['column_names']
    link_columns = info['link_columns']
    non_link_columns = [i for i in df.columns if i not in link_columns]
    for col in link_columns:
        new_col = col + '_link'
        df[new_col] = df.apply(lambda row: row[col][1] if isinstance(row[col], tuple) and len(row[col]) > 1 else None, axis=1)
        df[col] = df.apply(lambda row: row[col][0] if isinstance(row[col], tuple) else row[col], axis=1)
    
    for col in non_link_columns:
        df[col] = df.apply(lambda row: row[col][0] if isinstance(row[col], tuple) else row[col], axis=1)

    df = df.rename(columns={'game_date_link': 'box_score_link'})
    save_name = os.path.split(fp)[1]
    save_folder = 'data/team_schedules'
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    save_path = os.path.join(save_folder, save_name)
    df.to_pickle(save_path)
    return df
    
