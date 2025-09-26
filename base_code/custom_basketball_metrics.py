import numpy as np
import pandas as pd

def assist_to_turnover_ratio(row):
    asts = row['assists']
    tos = row['turnovers']
    if tos == 0:
        new_tos = 0.5
        final = asts/new_tos
    else:
        final = asts/tos
    return round(final, 4)

def playmaking_efficiency_score(row):
    ratio = assist_to_turnover_ratio(row)
    apg = row['assists_36']/row['games_played']
    return round(apg * ratio, 4)

def convert_to_per_36(metric_col, minutes_col):
    if minutes_col == 0:
        return None
    num = metric_col/minutes_col * 36
    return round(num, 4)

def usage_rate(row):
    #Usage Rate = [(FGA + (FTA × 0.44) + TOV) / (Team FGA + (Team FTA × 0.44) + Team TOV)] × 100
    player_score = row['player_field_goal_attempts'] + (row['player_free_throw_attempts'] * 0.44 + row['player_turnovers']) 
    team_score = row['team_field_goal_attempts'] + (row['team_free_throw_attempts'] * 0.44 + row['team_turnovers']) 
    score = player_score/team_score
    return round(score, 4)

def rank_df(df, dim_cols):
    new_df = df.copy()
    rank_cols = [i for i in new_df.columns if i not in dim_cols and '_rank' not in i]
    for col in rank_cols:
        new_col = col + '_rank'
        new_df[new_col] = new_df[col].rank(method='dense', ascending=False)
    return new_df

def defensive_efficiency_rating(row):
    # (Steals + Blocks + Def Rebounds) ÷ Fouls × 100
    if row['personal_fouls'] > 0:
        stls = row['steals']
        blks = row['blocks']
        def_rebounds = row['defensive_rebounds']
        fouls = row['personal_fouls']
        num = (stls + blks + def_rebounds)/fouls
    else:
        stls = row['steals']
        blks = row['blocks']
        def_rebounds = row['defensive_rebounds']
        fouls = 1
        num = (stls + blks + def_rebounds)/fouls
    return round(num, 4)

def true_shooting_percentage(row):
    points = row['points']
    fga = row['field_goal_attempts']
    fta = row['free_throw_attempts']
    if fga + fta == 0:
        return 0

    denom = 2 * (fga + (0.44 * fta))

    return round(points/denom, 4)

def effective_field_goal_percentage(row):
    fgs = row['field_goals']
    fgs_3 = row['field_goals_threes']
    atts = row['field_goal_attempts']
    if atts == 0:
        return np.nan
    numerator = fgs + (0.5 * fgs_3)
    return round(numerator/atts, 4) * 100


def calculate_weighted_averages_simple(df, groupby_cols):
    """
    Simple method to calculate weighted averages
    """
    
    # Filter out games with 0 minutes
    df_played = df[df['minutes_played'] > 0].copy()
    
    # Group by player and calculate weighted averages
    result = df_played.groupby(groupby_cols).apply(
        lambda group: pd.Series({
            # Basic info
            
            # Weighted averages for each metric
            'defensive_rebound_pct': np.average(group['defensive_rebound_pct'], weights=group['minutes_played']),
            'offensive_rebound_pct': np.average(group['offensive_rebound_pct'], weights=group['minutes_played']),
            'assist_pct': np.average(group['assist_pct'], weights=group['minutes_played']),
            'steal_pct': np.average(group['steal_pct'], weights=group['minutes_played']),
            'usage_rate': np.average(group['usage_rate'], weights=group['minutes_played']),
            'offensive_rating': np.average(group['offensive_rating'], weights=group['minutes_played']),
            'defensive_rating': np.average(group['defensive_rating'], weights=group['minutes_played'])
        })
    ).reset_index()
    
    return result

def pure_point_rating(row):
    #PPR = (Assists × 3) + (Steals × 2) - Turnovers
    assists = row['assists']
    steals = row['steals']
    tos = row['turnovers']
    final = (assists*3) + (steals * 2) - tos
    return final


def perimeter_impact_score(row):
    # GIS = (Points × 1.0) + (Assists × 1.5) + (Rebounds × 1.2) + 
    #   (Steals × 2.0) + (Blocks × 2.0) - (Turnovers × 1.0)
    points = row['points']
    assists = row['assists'] * 1.5
    rebounds = row['total_rebounds'] * 1.2
    steals = row['steals'] * 2
    blocks = row['blocks'] * 2
    tos = row['turnovers']
    final = points + assists + rebounds + steals + blocks - tos
    return final

def interior_impact_score(row):
    # Points × 1.0) + (Rebounds × 1.5) + (Blocks × 2.0) + 
    #           (Assists × 2.0) - (Turnovers × 1.0)
    points = row['points']
    assists = row['assists'] * 2
    rebounds = row['total_rebounds'] * 1.5
    blocks = row['blocks'] * 2
    tos = row['turnovers']
    final = points + assists + rebounds + blocks - tos
    return final


def scoring_efficiency_index(row):
    ## Formula: (Points × True Shooting %) / Usage Rate
    pts = row['points']
    tsp = row['true_shooting_pct']
    usage_rate = row['usage_rate']
    if usage_rate > 0:
        number = (pts * tsp)/usage_rate
    else:
        number = 0
    return round(number, 4)


def rebounding_impact_index(row):
    # - Centers/Forwards: (Offensive Rebound % × 1.5) + (Defensive Rebound % × 1.0)
    # - Guards: (Offensive Rebound % × 2.0) + (Defensive Rebound % × 1.5)
    if row['is_perimeter']:
        orbs = row['offensive_rebounds'] * 2
        drbs = row['defensive_rebounds'] * 1.5
    else:
        orbs = row['offensive_rebounds'] * 1.5
        drbs = row['defensive_rebounds'] 

    return orbs + drbs


def win_shares_estimate(row):
    ortg = row['offensive_rating']
    drtg = row['defensive_rating']
    num = ortg - drtg
    return round(num/10)


def clutch_index(row):
    wse = win_shares_estimate(row)
    gps = row['games_played']
    return round(wse/gps, 4)


def playmaking_index(row):
    # Guards: (Assists × Assist %) - (Turnovers × 1.5)
    # Forwards/Centers: (Assists × Assist %) - Turnovers
    if row['is_perimeter']:
        tos = row['turnovers'] * 1.5
    else:
        tos = row['turnovers']

    assists = row['assists']
    assist_pct = row['assist_pct']/100

    if tos == 0:
        return 0

    num = assists * assist_pct

    return round(num/tos, 4)


def defensive_impact_index(row):
    stls = row['steals']
    blks = row['blocks']
    drtg = row['defensive_rating']
    num = (stls + blks) * (100 - drtg)
    return round(num/100, 4)


def productivity_per_36(row):
    total = row['points'] + row['assists'] + row['total_rebounds'] + row['blocks'] + row['steals']
    num = (total/row['minutes_played']) * 36
    return round(num, 4)
    