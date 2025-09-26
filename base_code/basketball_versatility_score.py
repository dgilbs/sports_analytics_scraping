import pandas as pd
import numpy as np

def add_versatility_score_to_dataframe(df):
    """
    Takes a dataframe with player stats and adds versatility_score column.
    
    Parameters:
    df: DataFrame containing player statistics with required columns
    
    Returns:
    DataFrame with added 'versatility_score' column
    """
    
    # Create a copy to avoid modifying original dataframe
    df_with_versatility = df.copy()
    
    # Calculate per-game stats (if not already calculated)
    if 'points_per_game' not in df_with_versatility.columns:
        df_with_versatility['points_per_game'] = df_with_versatility['points'] / df_with_versatility['games_played']
    
    if 'rebounds_per_game' not in df_with_versatility.columns:
        df_with_versatility['rebounds_per_game'] = df_with_versatility['total_rebounds'] / df_with_versatility['games_played']
    
    if 'assists_per_game' not in df_with_versatility.columns:
        df_with_versatility['assists_per_game'] = df_with_versatility['assists'] / df_with_versatility['games_played']
    
    if 'steals_per_game' not in df_with_versatility.columns:
        df_with_versatility['steals_per_game'] = df_with_versatility['steals'] / df_with_versatility['games_played']
    
    if 'blocks_per_game' not in df_with_versatility.columns:
        df_with_versatility['blocks_per_game'] = df_with_versatility['blocks'] / df_with_versatility['games_played']
    
    # Calculate shooting percentages
    if 'fg_pct' not in df_with_versatility.columns:
        df_with_versatility['fg_pct'] = np.where(
            df_with_versatility['field_goal_attempts'] > 0,
            df_with_versatility['field_goals'] / df_with_versatility['field_goal_attempts'],
            0
        )
    
    if 'three_pt_pct' not in df_with_versatility.columns:
        df_with_versatility['three_pt_pct'] = np.where(
            df_with_versatility['field_goal_threes_attempts'] > 0,
            df_with_versatility['field_goals_threes'] / df_with_versatility['field_goal_threes_attempts'],
            0
        )
    
    # Define versatility categories
    versatility_categories = [
        'points_per_game',
        'rebounds_per_game',
        'assists_per_game', 
        'steals_per_game',
        'blocks_per_game',
        'fg_pct',
        'three_pt_pct'
    ]
    
    # Calculate position averages
    position_averages = df_with_versatility.groupby('playing_position')[versatility_categories].mean()
    
    # Initialize versatility score column
    df_with_versatility['versatility_score'] = 0
    
    # For each player, check how many categories they're above position average
    for idx, player in df_with_versatility.iterrows():
        player_position = player['playing_position']
        score = 0
        
        for category in versatility_categories:
            # Get position average for this category
            if player_position in position_averages.index:
                pos_avg = position_averages.loc[player_position, category]
                
                # Check if player exceeds position average
                if player[category] > pos_avg:
                    score += 1
        
        # Assign versatility score to this player
        df_with_versatility.at[idx, 'versatility_score'] = score
    
    return df_with_versatility