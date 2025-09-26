import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns



class PlayerPassingBenchmarking:
    def __init__(self, data):
        self.data = data.copy()
        self.benchmarks = {}
        self.key_metrics = [
            'passes_completed', 'passes_attempted', 'total_pass_distance',
            'total_progressive_pass_distance', 'assists', 'xag', 'xa',
            'key_passes', 'passes_into_final_third', 'passes_into_penalty_area',
            'progressive_passes'
        ]
        
    def calculate_per_90_metrics(self):
        """Calculate per-90-minute metrics for fair comparison"""
        print("Calculating per-90 metrics...")
        
        # Filter out rows with 0 minutes to avoid division by zero
        valid_minutes = self.data['minutes'] > 0
        print(f"Rows with valid minutes: {valid_minutes.sum()} out of {len(self.data)}")
        
        for metric in self.key_metrics:
            if metric in self.data.columns:
                # Only calculate for rows with valid minutes
                self.data[f'{metric}_per_90'] = np.where(
                    valid_minutes,
                    (self.data[metric] / self.data['minutes']) * 90,
                    np.nan
                )
                print(f"Created {metric}_per_90")
        
        # Calculate pass completion rates (avoid division by zero)
        self.data['pass_completion_rate'] = np.where(
            self.data['passes_attempted'] > 0,
            (self.data['passes_completed'] / self.data['passes_attempted']) * 100,
            np.nan
        )
        
        self.data['short_pass_completion_rate'] = np.where(
            self.data['short_passes_attempted'] > 0,
            (self.data['short_passes_completed'] / self.data['short_passes_attempted']) * 100,
            np.nan
        )
        
        self.data['medium_pass_completion_rate'] = np.where(
            self.data['medium_passes_attempted'] > 0,
            (self.data['medium_passes_completed'] / self.data['medium_passes_attempted']) * 100,
            np.nan
        )
        
        self.data['long_pass_completion_rate'] = np.where(
            self.data['long_passes_attempted'] > 0,
            (self.data['long_passes_completed'] / self.data['long_passes_attempted']) * 100,
            np.nan
        )
        
        print("Per-90 metrics calculated successfully")
        
    def create_positional_benchmarks(self, min_minutes=100):
        """Create benchmarks by position (minimum minutes played for inclusion)"""
        print(f"\nCreating positional benchmarks (min {min_minutes} minutes)...")
        
        # Filter for players with sufficient minutes
        qualified_data = self.data[self.data['minutes'] >= min_minutes].copy()
        print(f"Qualified players: {len(qualified_data)} out of {len(self.data)}")
        
        if len(qualified_data) == 0:
            print("WARNING: No players meet the minimum minutes requirement!")
            print(f"Minutes distribution: {self.data['minutes'].describe()}")
            # Try with lower threshold
            min_minutes = self.data['minutes'].quantile(0.5)  # Use median
            qualified_data = self.data[self.data['minutes'] >= min_minutes].copy()
            print(f"Trying with {min_minutes:.0f} minutes: {len(qualified_data)} players")
        
        # Group by primary position
        position_groups = qualified_data.groupby('primary_position')
        print(f"Positions found: {list(position_groups.groups.keys())}")
        
        per_90_metrics = [col for col in qualified_data.columns if '_per_90' in col]
        completion_metrics = [col for col in qualified_data.columns if 'completion_rate' in col]
        
        print(f"Per-90 metrics: {per_90_metrics}")
        print(f"Completion metrics: {completion_metrics}")
        
        benchmark_metrics = per_90_metrics + completion_metrics
        
        benchmarks = {}
        for position, group in position_groups:
            print(f"\nProcessing {position}: {len(group)} players")
            benchmarks[position] = {}
            
            for metric in benchmark_metrics:
                if metric in group.columns and not group[metric].isna().all():
                    valid_values = group[metric].dropna()
                    if len(valid_values) > 0:
                        benchmarks[position][metric] = {
                            'mean': valid_values.mean(),
                            'median': valid_values.median(),
                            'std': valid_values.std(),
                            'percentile_25': valid_values.quantile(0.25),
                            'percentile_75': valid_values.quantile(0.75),
                            'percentile_90': valid_values.quantile(0.90),
                            'percentile_95': valid_values.quantile(0.95),
                            'count': len(valid_values)
                        }
                        print(f"  {metric}: {len(valid_values)} valid values")
        
        self.benchmarks['positional'] = benchmarks
        print(f"\nPositional benchmarks created for {len(benchmarks)} positions")
        return benchmarks
    
    def create_league_benchmarks(self, min_minutes=100):
        """Create overall league benchmarks"""
        qualified_data = self.data[self.data['minutes'] >= min_minutes].copy()
        per_90_metrics = [col for col in qualified_data.columns if '_per_90' in col]
        completion_metrics = [col for col in qualified_data.columns if 'completion_rate' in col]
        
        benchmark_metrics = per_90_metrics + completion_metrics
        
        benchmarks = {}
        for metric in benchmark_metrics:
            if metric in qualified_data.columns and not qualified_data[metric].isna().all():
                benchmarks[metric] = {
                    'mean': qualified_data[metric].mean(),
                    'median': qualified_data[metric].median(),
                    'std': qualified_data[metric].std(),
                    'percentile_25': qualified_data[metric].quantile(0.25),
                    'percentile_75': qualified_data[metric].quantile(0.75),
                    'percentile_90': qualified_data[metric].quantile(0.90),
                    'percentile_95': qualified_data[metric].quantile(0.95),
                    'count': len(qualified_data[metric].dropna())
                }
        
        self.benchmarks['league'] = benchmarks
        return benchmarks
    
    def score_player_performance(self, player_name, position=None):
        """Score a player's performance against benchmarks"""
        player_data = self.data[self.data['player'] == player_name].copy()
        
        if len(player_data) == 0:
            return f"Player {player_name} not found in dataset"
        
        # Aggregate player's season performance
        player_stats = {
            'total_minutes': player_data['minutes'].sum(),
            'total_appearances': len(player_data)
        }
        
        # Calculate per-90 averages
        for metric in self.key_metrics:
            if metric in player_data.columns:
                total_metric = player_data[metric].sum()
                player_stats[f'{metric}_per_90'] = (total_metric / player_stats['total_minutes']) * 90
        
        # Calculate completion rates
        player_stats['pass_completion_rate'] = (
            player_data['passes_completed'].sum() / player_data['passes_attempted'].sum() * 100
        )
        
        # Determine position for benchmarking
        if position is None:
            position = player_data['primary_position'].mode().iloc[0]
        
        # Score against positional benchmarks
        scores = {}
        if position in self.benchmarks.get('positional', {}):
            pos_benchmarks = self.benchmarks['positional'][position]
            
            for metric, benchmark in pos_benchmarks.items():
                if metric in player_stats:
                    player_value = player_stats[metric]
                    # Calculate percentile score
                    z_score = (player_value - benchmark['mean']) / benchmark['std']
                    percentile = stats.norm.cdf(z_score) * 100
                    scores[metric] = {
                        'value': player_value,
                        'position_mean': benchmark['mean'],
                        'percentile': percentile,
                        'rating': self._get_rating(percentile)
                    }
        
        return {
            'player': player_name,
            'position': position,
            'season_stats': player_stats,
            'benchmark_scores': scores
        }
    
    def _get_rating(self, percentile):
        """Convert percentile to rating"""
        if percentile >= 95:
            return "Elite"
        elif percentile >= 90:
            return "Excellent"
        elif percentile >= 75:
            return "Above Average"
        elif percentile >= 50:
            return "Average"
        elif percentile >= 25:
            return "Below Average"
        else:
            return "Poor"
    
    def create_benchmark_report(self, top_n=10):
        """Create a comprehensive benchmark report"""
        report = {
            'league_leaders': {},
            'positional_leaders': {}
        }
        
        # Calculate per-90 metrics first
        self.calculate_per_90_metrics()
        
        # Filter qualified players
        qualified = self.data[self.data['minutes'] >= 450].copy()
        
        # League leaders
        key_per_90_metrics = [
            'passes_completed_per_90', 'progressive_passes_per_90',
            'assists_per_90', 'key_passes_per_90', 'xa_per_90'
        ]
        
        for metric in key_per_90_metrics:
            if metric in qualified.columns:
                top_players = qualified.nlargest(top_n, metric)[['player', 'primary_position', metric]]
                report['league_leaders'][metric] = top_players.to_dict('records')
        
        # Positional leaders
        for position in qualified['primary_position'].unique():
            pos_data = qualified[qualified['primary_position'] == position]
            report['positional_leaders'][position] = {}
            
            for metric in key_per_90_metrics:
                if metric in pos_data.columns and len(pos_data) >= 5:
                    top_players = pos_data.nlargest(min(5, len(pos_data)), metric)[['player', metric]]
                    report['positional_leaders'][position][metric] = top_players.to_dict('records')
        
        return report
    
    def visualize_player_comparison(self, players, metrics=None):
        """Create radar chart comparing multiple players"""
        if metrics is None:
            metrics = [
                'passes_completed_per_90', 'progressive_passes_per_90',
                'assists_per_90', 'key_passes_per_90', 'pass_completion_rate'
            ]
        
        # Calculate per-90 metrics
        self.calculate_per_90_metrics()
        
        fig, ax = plt.subplots(figsize=(10, 8), subplot_kw=dict(projection='polar'))
        
        angles = np.linspace(0, 2 * np.pi, len(metrics), endpoint=False).tolist()
        angles += angles[:1]  # Complete the circle
        
        for player in players:
            player_data = self.data[self.data['player'] == player]
            if len(player_data) == 0:
                continue
                
            # Calculate player averages
            values = []
            for metric in metrics:
                if metric == 'pass_completion_rate':
                    value = (player_data['passes_completed'].sum() / 
                            player_data['passes_attempted'].sum() * 100)
                else:
                    total_metric = player_data[metric.replace('_per_90', '')].sum()
                    value = (total_metric / player_data['minutes'].sum()) * 90
                values.append(value)
            
            values += values[:1]  # Complete the circle
            
            ax.plot(angles, values, 'o-', linewidth=2, label=player)
            ax.fill(angles, values, alpha=0.25)
        
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels([m.replace('_per_90', '').replace('_', ' ').title() for m in metrics])
        ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.0))
        
        plt.title('Player Performance Comparison', size=16, fontweight='bold', pad=20)
        plt.tight_layout()
        return fig

# Example usage