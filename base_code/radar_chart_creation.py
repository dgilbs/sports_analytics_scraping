import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import percentileofscore

def get_ordinal_suffix(number):
    """
    Get the correct ordinal suffix for a number (st, nd, rd, th)
    """
    if 10 <= number % 100 <= 20:  # Special case for 11th, 12th, 13th
        return "th"
    else:
        last_digit = number % 10
        if last_digit == 1:
            return "st"
        elif last_digit == 2:
            return "nd"
        elif last_digit == 3:
            return "rd"
        else:
            return "th"

def create_player_radar_chart(df, player_name, stats=None, lower_is_better=None, 
                             stat_labels=None, title=None, subtitle=None, save_path=None, figsize=(10, 8)):
    """
    Create a radar chart comparing a player's stats to the average of other players.
    
    Parameters:
    -----------
    csv_file : str
        Path to the CSV file containing player data
    player_name : str
        Name of the player to highlight (must match exactly as in CSV)
    stats : list, optional
        List of column names to include in radar chart. If None, uses all numeric columns
    lower_is_better : list, optional
        List of stat names where lower values are better (e.g., ['errors', 'fouls'])
    stat_labels : list, optional
        Custom labels for the stats. If None, uses column names with formatting
    title : str, optional
        Custom title for the chart. If None, creates default title
    subtitle : str, optional
        Subtitle text to appear below the main title
    save_path : str, optional
        Path to save the chart. If None, only displays the chart
    figsize : tuple, optional
        Figure size as (width, height). Default is (10, 8)
    
    Returns:
    --------
    fig, ax : matplotlib figure and axis objects
    """
    
    # If no stats specified, use all numeric columns except 'player'
    if stats is None:
        stats = df.select_dtypes(include=[np.number]).columns.tolist()
        if 'player' in stats:
            stats.remove('player')
    
    # Convert specified columns to numeric, handling any conversion issues
    for col in stats:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Set default for lower_is_better
    if lower_is_better is None:
        lower_is_better = []
    
    # Create readable labels if not provided
    if stat_labels is None:
        stat_labels = [stat.replace('_', ' ').title().replace(' ', '\n') for stat in stats]
    
    # Find player's data
    player_data = df[df['player'] == player_name]
    if player_data.empty:
        raise ValueError(f"Player '{player_name}' not found in the dataset")
    
    player_values = player_data[stats].values[0]
    
    # Calculate the average values for all other players
    avg_values = df[stats].mean().values
    
    # Calculate max and min values for normalization
    max_values = df[stats].max().values
    min_values = df[stats].min().values
    
    # Calculate percentile ranks for visualization
    player_percentiles = []
    avg_percentiles = []
    
    for i, stat in enumerate(stats):
        # Get all values for this stat
        stat_values = df[stat].dropna().values
        
        if stat in lower_is_better:
            # For "lower is better" stats, we want lower values to have higher percentiles
            # Use 'rank' method which handles ties properly
            player_percentile = (100 - percentileofscore(stat_values, player_values[i], kind='rank')) / 100
            avg_percentile = (100 - percentileofscore(stat_values, avg_values[i], kind='rank')) / 100
        else:
            # For "higher is better" stats, calculate normal percentile rank
            player_percentile = percentileofscore(stat_values, player_values[i], kind='rank') / 100
            avg_percentile = percentileofscore(stat_values, avg_values[i], kind='rank') / 100
        player_percentiles.append(player_percentile)
        avg_percentiles.append(avg_percentile)
    
    player_percentiles = np.array(player_percentiles)
    avg_percentiles = np.array(avg_percentiles)
    
    # Set up the angles for the radar chart
    angles = np.linspace(0, 2*np.pi, len(stat_labels), endpoint=False).tolist()
    
    # Close the plot
    angles += [angles[0]]
    player_percentiles = np.append(player_percentiles, player_percentiles[0])
    avg_percentiles = np.append(avg_percentiles, avg_percentiles[0])
    labels_closed = stat_labels + [stat_labels[0]]
    
    # Create the plot
    fig, ax = plt.subplots(figsize=figsize, subplot_kw=dict(polar=True))
    
    # Plot data
    ax.plot(angles, player_percentiles, 'r-', linewidth=2, label=player_name)
    ax.fill(angles, player_percentiles, 'r', alpha=0.3)
    
    ax.plot(angles, avg_percentiles, 'b-', linewidth=2, label='Other Players Avg')
    ax.fill(angles, avg_percentiles, 'b', alpha=0.1)
    
    # Fix axis to go in the right order and start at 12 o'clock
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    
    # Draw axis lines for each angle and label
    ax.set_thetagrids(np.degrees(angles[:-1]), labels_closed[:-1])
    
    # Dynamically adjust label positions based on their angle
    for label, angle in zip(ax.get_xticklabels(), angles[:-1]):
        angle_rad = (-angle + np.pi/2) % (2*np.pi)
        
        if np.pi/4 <= angle_rad < 3*np.pi/4:  # Left side
            label.set_horizontalalignment('right')
            label.set_position((label._x - 0.1, label._y - 0.05))
        elif 3*np.pi/4 <= angle_rad < 5*np.pi/4:  # Bottom
            label.set_horizontalalignment('center')
            label.set_verticalalignment('top')
            label.set_position((label._x, label._y - 0.1))
        elif 5*np.pi/4 <= angle_rad < 7*np.pi/4:  # Right side
            label.set_horizontalalignment('left')
            label.set_position((label._x + 0.1, label._y - 0.1))
        else:  # Top
            label.set_horizontalalignment('center')
            label.set_verticalalignment('bottom')
            label.set_position((label._x - 0.1, label._y - 0.1))
    
    # Set y limits and labels - MODIFIED: Changed from 1.1 to 1.0 to end at 100%
    ax.set_ylim(0, 1.0)
    ax.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(['20%', '40%', '60%', '80%', '100%'])
    
    # MODIFIED: Remove the outer ring by turning off spines
    ax.spines['polar'].set_visible(False)
    
    ax.grid(True)
    
    # Add legend
    ax.legend(loc='upper right', bbox_to_anchor=(0.1, 0.1))
    
    # Add title and subtitle
    if title is None:
        title = f"{player_name} vs. Average of Other Players"
    
    if subtitle:
        # When there's a subtitle, adjust title positioning
        plt.title(title, size=15, y=1.08)
        plt.suptitle(subtitle, size=12, y=1.06, style='italic')
    else:
        # Default title position when no subtitle
        plt.title(title, size=15, y=1.1)
    # Add actual values annotation with percentile ranks
    value_text = ""
    for i, stat in enumerate(stats):
        indicator = " (↓)" if stat in lower_is_better else ""
        percentile = int(player_percentiles[i] * 100) - 1
        suffix = get_ordinal_suffix(percentile)
        value_text += f"{stat_labels[i].replace('\n', ' ')}{indicator}: {player_values[i]} ({percentile}{suffix} percentile)\n" 
    
    plt.figtext(0.99, 0.01, value_text, fontsize=9, horizontalalignment='right',
                verticalalignment='top', bbox=dict(facecolor='white', alpha=0.5, pad=5))
    
    plt.tight_layout()
    
    # Save or show the plot
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
    
    plt.show()
    
    return fig, ax

# Example usage:

# Basic usage with your current data
#fig, ax = create_player_radar_chart('outside_backs_attacking.csv', 'Perle Morroni')

# Advanced usage with custom parameters
# stats_to_use = ['progressive_carries', 'key_passes', 'take_ons_succeeded']
# custom_labels = ['Progressive\nCarries', 'Key\nPasses', 'Take-ons\nSucceeded']
# lower_better = []  # Add any stats where lower is better, e.g., ['errors', 'fouls']
# 
# fig, ax = create_player_radar_chart(
#     csv_file='outside_backs_attacking.csv',
#     player_name='Perle Morroni',
#     stats=stats_to_use,
#     stat_labels=custom_labels,
#     lower_is_better=lower_better,
#     title="Perle Morroni - Attacking Metrics",
#     subtitle="2024 NWSL Season | Fullback Performance Analysis",
#     save_path='morroni_radar.png'
# )

# Quick comparison of different players
# create_player_radar_chart('outside_backs_attacking.csv', 'Hanna Lundkvist')
# create_player_radar_chart('outside_backs_attacking.csv', 'Gabrielle Carle')