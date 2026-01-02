import pandas as pd
import yaml
import sqlite3
import psycopg2
import os
import random
import time
import itertools
import matplotlib.pyplot as plt
from datetime import date, datetime, timedelta
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_connection():
    """Get database connection from environment variable"""
    conn_string = os.getenv('DATABASE_URL')
    if not conn_string:
        raise ValueError("DATABASE_URL not found in environment variables. Please set it in your .env file.")
    return psycopg2.connect(conn_string)

def retrieve_table(table_name, db_config):
    """Retrieve a table from the nwsfl schema"""
    conn = get_connection()
    query = "select * from nwsfl.{}".format(table_name)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_all_nwsfl_tables():
    """Get all tables in the nwsfl schema"""
    conn = get_connection()
    cur = conn.cursor()
    query = "SELECT table_name FROM information_schema.tables where table_schema = 'nwsfl'"
    cur.execute(query)
    results = cur.fetchall()
    tbl_names = [i[0] for i in results if 'dim_' in i[0] or i[0][:2] == 'f_']
    conn.close()
    return tbl_names

def get_all_nwsfl_views():
    """Get all views in the nwsfl schema"""
    conn = get_connection()
    cur = conn.cursor()
    query = "SELECT table_name FROM information_schema.views where table_schema = 'nwsfl'"
    cur.execute(query)
    results = cur.fetchall()
    tbl_names = [i[0] for i in results]
    conn.close()
    return tbl_names

# Keep old function names for backward compatibility
def get_all_soccer_tables():
    """Deprecated: Use get_all_nwsfl_tables() instead"""
    return get_all_nwsfl_tables()

def get_all_soccer_views():
    """Deprecated: Use get_all_nwsfl_views() instead"""
    return get_all_nwsfl_views()

def get_all_basketball_tables():
    """Get all tables in the basketball schema"""
    conn = get_connection()
    cur = conn.cursor()
    query = "SELECT table_name FROM information_schema.tables where table_schema = 'basketball'"
    cur.execute(query)
    results = cur.fetchall()
    tbl_names = [i[0] for i in results if 'dim_' in i[0] or i[0][:2] == 'f_']
    conn.close()
    return tbl_names

def add_keys_to_tables(schema, table_list):
    """Add primary keys to tables in a schema"""
    for table in table_list:
        conn = get_connection()
        cur = conn.cursor()
        try:
            table_id = schema + '.' + table
            query = """
                        alter table {} add primary key (id)

                    """.format(table_id)
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)
        conn.close()

def rank_df(df, dim_cols):
    new_df = df.copy()
    rank_cols = [i for i in new_df.columns if i not in dim_cols and '_rank' not in i]
    for col in rank_cols:
        new_col = col + '_rank'
        new_df[new_col] = new_df[col].rank(method='dense', ascending=False)
    return new_df

def find_best_match(x, choices):
    match, score, _ = process.extractOne(x, choices)
    # if score > 90:
    #     return match
    # else:
    #     return None
    return match

def query_to_dataframe(query, connection_string):
    """
    Execute SQL query and return DataFrame.
    
    Args:
        query (str): SQL query to execute
        connection_string (str): PostgreSQL connection string
            Format: 'postgresql://user:password@host:port/database'
    
    Returns:
        pandas.DataFrame: Query results
    """
    engine = create_engine(connection_string)
    df = pd.read_sql_query(query, engine)
    engine.dispose()
    return df

def pull_all_nwsfl_reporting_tables(conn_string):
    """Pull all nwsfl reporting tables and save to CSV"""
    tables = ['summary', 'passing', 'defense', 'possession', 'passing_types', 'full_stats']
    for table in tables:
        table_name = 'player_reporting_{}'.format(table)
        query = """
            select * from nwsfl.{}
            
        """.format(table_name)
        df = query_to_dataframe(query, conn_string)
        if not os.path.exists('data'):
            os.makedirs('data')

        fp = 'data/{}_all_players.csv'.format(table_name)
        df.to_csv(fp, index=False)

        for_df = df[df.is_forward]
        for_fp = 'data/{}_forwards.csv'.format(table)
        for_df.to_csv(for_fp, index=False)

        mid_df = df[df.is_midfielder]
        mid_fp = 'data/{}_midfielders.csv'.format(table)
        mid_df.to_csv(mid_fp, index=False)

        def_df = df[df.is_defender]
        def_fp = 'data/{}_defenders.csv'.format(table)
        def_df.to_csv(def_fp, index=False)
        
    return True

# Keep old function name for backward compatibility
def pull_all_soccer_reporting_tables(conn_string):
    """Deprecated: Use pull_all_nwsfl_reporting_tables() instead"""
    return pull_all_nwsfl_reporting_tables(conn_string)

def pull_basketball_reporting_tables(conn_string):

    if not os.path.exists('data'):
        os.makedirs('data')
        
    tables = ['basic_stats', 'advanced_stats']

    for table in tables:
        query = """
            select * from basketball.player_{}_reporting
        
        """.format(table)

        temp = query_to_dataframe(query, conn_string)
        fp = 'data/{}_all_players.csv'.format(table)
        temp.to_csv(fp, index=False)

        posts = temp[temp.is_post]
        post_fp = 'data/{}_posts.csv'.format(table)
        posts.to_csv(post_fp, index=False)

        peri = temp[temp.is_perimeter]
        peri_fp = 'data/{}_perimeters.csv'.format(table)
        peri.to_csv(peri_fp, index=False)
    return True

def backup_all_basketball_tables(conn_string, schema='basketball'):
    tables = get_all_basketball_tables()
    for table in tables:
        query = """
        
            select * from {}.{}
        """.format(schema, table)
        df = query_to_dataframe(query, conn_string)
        folder = 'db_backup'
        if not os.path.exists(folder):
            os.makedirs(folder)
        file = '{}.csv'.format(table)
        fp = os.path.join(folder, file)
        df.to_csv(fp, index=False)
    return True
    
def backup_all_nwsfl_tables(conn_string, schema='nwsfl'):
    """Backup all nwsfl tables to CSV files"""
    tables = get_all_nwsfl_tables()
    for table in tables:
        query = """
        
            select * from {}.{}
        """.format(schema, table)
        df = query_to_dataframe(query, conn_string)
        folder = 'db_backup'
        if not os.path.exists(folder):
            os.makedirs(folder)
        file = '{}.csv'.format(table)
        fp = os.path.join(folder, file)
        df.to_csv(fp, index=False)
    return True

# Keep old function name for backward compatibility  
def backup_all_soccer_tables(conn_string, schema='nwsfl'):
    """Deprecated: Use backup_all_nwsfl_tables() instead"""
    return backup_all_nwsfl_tables(conn_string, schema)


def pull_all_analytics_nwsfl_tables(conn_string):
    """Pull all nwsfl analytics views and save to CSV"""
    tables = get_all_nwsfl_views()
    analytics = [i for i in tables if 'analytics_' in i]
    if not os.path.exists('chart_data'):
        os.makedirs('chart_data')
        
    for i in analytics:
        query = """
            select * from nwsfl.{}
        
        """.format(i)
        df = query_to_dataframe(query, conn_string)
        file_name = i + '.csv'
        fp = os.path.join('chart_data', file_name)
        df.to_csv(fp, index=False)

    return True

# Keep old function name for backward compatibility
def pull_all_analytics_soccer_tables(conn_string):
    """Deprecated: Use pull_all_analytics_nwsfl_tables() instead"""
    return pull_all_analytics_nwsfl_tables(conn_string)

        
        