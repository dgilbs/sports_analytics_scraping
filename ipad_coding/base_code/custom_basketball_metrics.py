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

    return round(points/denom * 100, 4)

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
