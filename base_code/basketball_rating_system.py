import pandas as pd
import os
import sys
import yaml
import numpy as np
import requests
current_dir = os.getcwd()
parent_dir = os.path.abspath(os.path.join(current_dir, '../base_code/'))
sys.path.append(parent_dir)
import query_db as qdb
import warnings
from scipy.stats import percentileofscore
import random
from functools import reduce
from scipy import stats
from datetime import date
from io import StringIO

class PlayerRatings:

    def __init__(self, benchmark_data, scoring_data):
        self.benchmark_data = benchmark_data
        self.scoring_data = scoring_data
        self.key_metrics = ['points', 'assists']

    def add_metrics(self):
        # Original metric
        self.benchmark_data['assist_to_turnover_ratio'] = self.benchmark_data.apply(lambda row: self._assist_to_turnover_ratio(row), axis=1)
        self.scoring_data['assist_to_turnover_ratio'] = self.scoring_data.apply(lambda row: self._assist_to_turnover_ratio(row), axis=1)
        
        # New metrics from custom_basketball_metrics.py
        self.benchmark_data['playmaking_efficiency_score'] = self.benchmark_data.apply(lambda row: self._playmaking_efficiency_score(row), axis=1)
        self.scoring_data['playmaking_efficiency_score'] = self.scoring_data.apply(lambda row: self._playmaking_efficiency_score(row), axis=1)
        
        # self.benchmark_data['usage_rate'] = self.benchmark_data.apply(lambda row: self._usage_rate(row), axis=1)
        # self.scoring_data['usage_rate'] = self.scoring_data.apply(lambda row: self._usage_rate(row), axis=1)
        
        self.benchmark_data['defensive_efficiency_rating'] = self.benchmark_data.apply(lambda row: self._defensive_efficiency_rating(row), axis=1)
        self.scoring_data['defensive_efficiency_rating'] = self.scoring_data.apply(lambda row: self._defensive_efficiency_rating(row), axis=1)
        
        self.benchmark_data['true_shooting_percentage'] = self.benchmark_data.apply(lambda row: self._true_shooting_percentage(row), axis=1)
        self.scoring_data['true_shooting_percentage'] = self.scoring_data.apply(lambda row: self._true_shooting_percentage(row), axis=1)
        
        self.benchmark_data['effective_field_goal_percentage'] = self.benchmark_data.apply(lambda row: self._effective_field_goal_percentage(row), axis=1)
        self.scoring_data['effective_field_goal_percentage'] = self.scoring_data.apply(lambda row: self._effective_field_goal_percentage(row), axis=1)
        
        self.benchmark_data['pure_point_rating'] = self.benchmark_data.apply(lambda row: self._pure_point_rating(row), axis=1)
        self.scoring_data['pure_point_rating'] = self.scoring_data.apply(lambda row: self._pure_point_rating(row), axis=1)
        
        self.benchmark_data['perimeter_impact_score'] = self.benchmark_data.apply(lambda row: self._perimeter_impact_score(row), axis=1)
        self.scoring_data['perimeter_impact_score'] = self.scoring_data.apply(lambda row: self._perimeter_impact_score(row), axis=1)
        
        self.benchmark_data['interior_impact_score'] = self.benchmark_data.apply(lambda row: self._interior_impact_score(row), axis=1)
        self.scoring_data['interior_impact_score'] = self.scoring_data.apply(lambda row: self._interior_impact_score(row), axis=1)
        
        self.benchmark_data['scoring_efficiency_index'] = self.benchmark_data.apply(lambda row: self._scoring_efficiency_index(row), axis=1)
        self.scoring_data['scoring_efficiency_index'] = self.scoring_data.apply(lambda row: self._scoring_efficiency_index(row), axis=1)
        
        self.benchmark_data['rebounding_impact_index'] = self.benchmark_data.apply(lambda row: self._rebounding_impact_index(row), axis=1)
        self.scoring_data['rebounding_impact_index'] = self.scoring_data.apply(lambda row: self._rebounding_impact_index(row), axis=1)
        
        self.benchmark_data['win_shares_estimate'] = self.benchmark_data.apply(lambda row: self._win_shares_estimate(row), axis=1)
        self.scoring_data['win_shares_estimate'] = self.scoring_data.apply(lambda row: self._win_shares_estimate(row), axis=1)
        
        self.benchmark_data['clutch_index'] = self.benchmark_data.apply(lambda row: self._clutch_index(row), axis=1)
        self.scoring_data['clutch_index'] = self.scoring_data.apply(lambda row: self._clutch_index(row), axis=1)
        
        self.benchmark_data['playmaking_index'] = self.benchmark_data.apply(lambda row: self._playmaking_index(row), axis=1)
        self.scoring_data['playmaking_index'] = self.scoring_data.apply(lambda row: self._playmaking_index(row), axis=1)
        
        self.benchmark_data['defensive_impact_index'] = self.benchmark_data.apply(lambda row: self._defensive_impact_index(row), axis=1)
        self.scoring_data['defensive_impact_index'] = self.scoring_data.apply(lambda row: self._defensive_impact_index(row), axis=1)
        
        self.benchmark_data['productivity_per_36'] = self.benchmark_data.apply(lambda row: self._productivity_per_36(row), axis=1)
        self.scoring_data['productivity_per_36'] = self.scoring_data.apply(lambda row: self._productivity_per_36(row), axis=1)

    def add_per_36_metrics(self):
        for col in self.key_metrics:
            new_col = '{}_36'.format(col)
            self.benchmark_data[new_col] = (self.benchmark_data[col]/self.benchmark_data['minutes_played']) * 36
            self.scoring_data[new_col] = (self.scoring_data[col]/self.scoring_data['minutes_played']) * 36

    def _assist_to_turnover_ratio(self, row):
        asts = row['assists']
        tos = row['turnovers']
        if tos == 0:
            new_tos = 0.5
            final = asts/new_tos
        else:
            final = asts/tos
        return round(final, 4)

    def _playmaking_efficiency_score(self, row):
        ratio = self._assist_to_turnover_ratio(row)
        # Note: This assumes assists_36 and games_played columns exist
        # You may need to adjust based on your actual column names
        if 'assists_36' in row and 'games_played' in row:
            apg = row['assists_36']/row['games_played']
        else:
            # Fallback calculation if assists_36 doesn't exist yet
            apg = (row['assists']/row['minutes_played']) * 36 / row.get('games_played', 1)
        return round(apg * ratio, 4)

    def _usage_rate(self, row):
        # Usage Rate = [(FGA + (FTA × 0.44) + TOV) / (Team FGA + (Team FTA × 0.44) + Team TOV)] × 100
        player_score = row['player_field_goal_attempts'] + (row['player_free_throw_attempts'] * 0.44 + row['player_turnovers']) 
        team_score = row['team_field_goal_attempts'] + (row['team_free_throw_attempts'] * 0.44 + row['team_turnovers']) 
        if team_score == 0:
            return 0
        score = player_score/team_score
        return round(score, 4)

    def _defensive_efficiency_rating(self, row):
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

    def _true_shooting_percentage(self, row):
        points = row['points']
        fga = row['field_goal_attempts']
        fta = row['free_throw_attempts']
        if fga + fta == 0:
            return 0
        denom = 2 * (fga + (0.44 * fta))
        return round(points/denom, 4)

    def _effective_field_goal_percentage(self, row):
        fgs = row['field_goals']
        fgs_3 = row['field_goals_threes']
        atts = row['field_goal_attempts']
        if atts == 0:
            return np.nan
        numerator = fgs + (0.5 * fgs_3)
        return round(numerator/atts, 4) * 100

    def _pure_point_rating(self, row):
        # PPR = (Assists × 3) + (Steals × 2) - Turnovers
        assists = row['assists']
        steals = row['steals']
        tos = row['turnovers']
        final = (assists*3) + (steals * 2) - tos
        return final

    def _perimeter_impact_score(self, row):
        # GIS = (Points × 1.0) + (Assists × 1.5) + (Rebounds × 1.2) + (Steals × 2.0) + (Blocks × 2.0) - (Turnovers × 1.0)
        points = row['points']
        assists = row['assists'] * 1.5
        rebounds = row['total_rebounds'] * 1.2
        steals = row['steals'] * 2
        blocks = row['blocks'] * 2
        tos = row['turnovers']
        final = points + assists + rebounds + steals + blocks - tos
        return final

    def _interior_impact_score(self, row):
        # Points × 1.0) + (Rebounds × 1.5) + (Blocks × 2.0) + (Assists × 2.0) - (Turnovers × 1.0)
        points = row['points']
        assists = row['assists'] * 2
        rebounds = row['total_rebounds'] * 1.5
        blocks = row['blocks'] * 2
        tos = row['turnovers']
        final = points + assists + rebounds + blocks - tos
        return final

    def _scoring_efficiency_index(self, row):
        # Formula: (Points × True Shooting %) / Usage Rate
        pts = row['points']
        tsp = self._true_shooting_percentage(row)
        usage_rate = self._usage_rate(row)
        if usage_rate > 0:
            number = (pts * tsp)/usage_rate
        else:
            number = 0
        return round(number, 4)

    def _rebounding_impact_index(self, row):
        # Centers/Forwards: (Offensive Rebound % × 1.5) + (Defensive Rebound % × 1.0)
        # Guards: (Offensive Rebound % × 2.0) + (Defensive Rebound % × 1.5)
        # Note: You'll need to determine how to identify perimeter vs interior players
        # This assumes an 'is_perimeter' column exists
        if row.get('is_perimeter', False):
            orbs = row['offensive_rebounds'] * 2
            drbs = row['defensive_rebounds'] * 1.5
        else:
            orbs = row['offensive_rebounds'] * 1.5
            drbs = row['defensive_rebounds']
        return orbs + drbs

    def _win_shares_estimate(self, row):
        ortg = row['offensive_rating']
        drtg = row['defensive_rating']
        num = ortg - drtg
        return round(num/10)

    def _clutch_index(self, row):
        wse = self._win_shares_estimate(row)
        gps = row['games_played']
        if gps == 0:
            return 0
        return round(wse/gps, 4)

    def _playmaking_index(self, row):
        # Guards: (Assists × Assist %) - (Turnovers × 1.5)
        # Forwards/Centers: (Assists × Assist %) - Turnovers
        if row.get('is_perimeter', False):
            tos = row['turnovers'] * 1.5
        else:
            tos = row['turnovers']

        assists = row['assists']
        assist_pct = row['assist_pct']/100

        if tos == 0:
            return 0

        num = assists * assist_pct
        return round(num/tos, 4)

    def _defensive_impact_index(self, row):
        stls = row['steals']
        blks = row['blocks']
        drtg = row['defensive_rating']
        num = (stls + blks) * (100 - drtg)
        return round(num/100, 4)

    def _productivity_per_36(self, row):
        total = row['points'] + row['assists'] + row['total_rebounds'] + row['blocks'] + row['steals']
        if row['minutes_played'] == 0:
            return 0
        num = (total/row['minutes_played']) * 36
        return round(num, 4)