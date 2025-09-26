import pandas as pd
import numpy as np
from scipy import stats

class PlayerRatings:

    def __init__(self, benchmark_data, scoring_data):
        self.benchmark_data = benchmark_data
        self.scoring_data = scoring_data
        self.key_metrics = [
            'shot_creation_index',
            'creative_playmaking_index',
            'ball_progression_index',
            'defensive_impact_index',
            'duel_winning_index',
            'tackling_index',
            'touches',
            'passes_completed',
            'xg',
            'xa',
            'npxg',
            'carries'
        ]

    def add_metrics(self):
        self.benchmark_data['shot_creation_index'] = self.benchmark_data.apply(lambda row: self._shot_creation_effiency(row), axis=1)
        self.scoring_data['shot_creation_index'] = self.scoring_data.apply(lambda row: self._shot_creation_effiency(row), axis=1)
        
         # Creative Playmaking Index
        self.benchmark_data['creative_playmaking_index'] = self.benchmark_data.apply(lambda row: self._creative_playmaking_index(row), axis=1)
        self.scoring_data['creative_playmaking_index'] = self.scoring_data.apply(lambda row: self._creative_playmaking_index(row), axis=1)
        
        self.benchmark_data['ball_progression_index'] = self.benchmark_data.apply(lambda row: self._ball_progression_mastery(row), axis=1)
        self.scoring_data['ball_progression_index'] = self.scoring_data.apply(lambda row: self._ball_progression_mastery(row), axis=1)
        
        # Defensive Impact Score
        self.benchmark_data['defensive_impact_index'] = self.benchmark_data.apply(lambda row: self._defensive_impact_score(row), axis=1)
        self.scoring_data['defensive_impact_index'] = self.scoring_data.apply(lambda row: self._defensive_impact_score(row), axis=1)
        
          # Duel Winning Ability
        self.benchmark_data['duel_winning_index'] = self.benchmark_data.apply(lambda row: self._duel_winning_ability(row), axis=1)
        self.scoring_data['duel_winning_index'] = self.scoring_data.apply(lambda row: self._duel_winning_ability(row), axis=1)

        self.benchmark_data['tackling_index'] = self.benchmark_data.apply(lambda row: self._tackling_score(row), axis=1)
        self.scoring_data['tackling_index'] = self.scoring_data.apply(lambda row: self._tackling_score(row), axis=1)




    def _shot_creation_effiency(self, row):
        scas = row['shot_creating_actions']
        gcas = row['goal_creating_actions']
        shots = row['shots']
        touches_att_third = row['touches_att_third']
        passes_into_final_third = row['passes_into_final_third']
        numerator = scas + gcas + shots
        denomenator = touches_att_third + passes_into_final_third
        if denomenator == 0:
          return 0

        result = round(numerator/denomenator, 4)
        return result

    def score_position_group(self, position_group):
        benchmark = self.benchmark_data[self.benchmark_data.position_group == position_group].copy()
        scoring = self.scoring_data[self.scoring_data.position_group == position_group].copy()
        
        if benchmark.empty or scoring.empty:
            return pd.DataFrame()
        
        score_cols = list()
        for i in self.key_metrics:
          cap = benchmark[i].quantile(0.90)
          benchmark[i] = benchmark[i].clip(upper=cap)
          arr = [i for i in benchmark[i] if not np.isnan(i) and not np.isinf(i)]
          if '_index' in i:
              score_col = i.replace('_index', '_score')
          else:
              score_col = i + '_score'
          scoring[score_col] = scoring.apply(lambda row: round(stats.percentileofscore(arr, row[i])/10, 3), axis=1)
          score_cols.append(score_col)
        
        dim_cols = ['player', 'squad', 'opponent', 'season', 'match_date', 'competition',
           'position_group', 'minutes']
        cols = dim_cols + score_cols
        
        scoring = scoring[cols]
        
        return scoring

    
    def _normalize_score(self, value, high_val, low_val):
        value = np.clip(value, a_min=low_val, a_max=high_val)
        scaled = 10 * (value - low_val) / (high_val - low_val)
        return scaled

    def _creative_playmaking_index(self, row):
        """
        Creative Playmaking Index
        (assists + key_passes + passes_into_penalty_area + crosses_into_penalty_area + xag) / passes_attempted
        Measures creative passing output relative to total passing volume
        """
        assists = row['assists']
        key_passes = row['key_passes']
        passes_into_penalty_area = row['passes_into_penalty_area']
        crosses_into_penalty_area = row['crosses_into_penalty_area']
        xag = row['xag']
        passes_attempted = row['passes_attempted']
        
        numerator = assists + key_passes + passes_into_penalty_area + crosses_into_penalty_area + xag
        denominator = passes_attempted
        
        if denominator == 0:
            return 0
        
        result = round(numerator / denominator, 4)
        return result

    def _ball_progression_mastery(self, row):
        """
        Ball Progression Mastery
        (progressive_passes + progressive_carries + passes_into_final_third + carries_into_final_third) / (passes_attempted + carries)
        Measures how often a player advances the ball through multiple methods
        """
        progressive_passes = row['progressive_passes']
        progressive_carries = row['progressive_carries']
        passes_into_final_third = row['passes_into_final_third']
        carries_into_final_third = row['carries_into_final_third']
        passes_attempted = row['passes_attempted']
        carries = row['carries']
        
        numerator = progressive_passes + progressive_carries + passes_into_final_third + carries_into_final_third
        denominator = passes_attempted + carries
        
        if denominator == 0:
            return 0
        
        result = round(numerator / denominator, 4)
        return result

    def _defensive_impact_score(self, row):
        """
        Defensive Impact Score
        (tackles_won + interceptions + blocks + clearances + aerial_duels_won) / (tackles_att + challenges_att + aerial_duels_won + aerial_duels_lost)
        Comprehensive defensive contribution weighted by attempts
        """
        tackles_won = row['tackles_won']
        interceptions = row['interceptions']
        blocks = row['blocks']
        clearances = row['clearances']
        aerial_duels_won = row['aerial_duels_won']
        tackles_att = row['tackles_att']
        challenges_att = row['challenges_att']
        aerial_duels_lost = row['aerial_duels_lost']
        
        numerator = tackles_won + interceptions + blocks + clearances + aerial_duels_won
        denominator = tackles_att + challenges_att + aerial_duels_won + aerial_duels_lost
        
        if denominator == 0:
          return 0
        
        result = round(numerator / denominator, 4)
        return result

    def _clinical_finishing(self, row):
        """
        Clinical Finishing
        (goals + shots_on_target) / (shots + xg)
        Combines actual finishing with shot quality, weighted by expected goals
        """
        goals = row['goals']
        shots_on_target = row['shots_on_target']
        shots = row['shots']
        xg = row['xg']
        
        numerator = goals + shots_on_target
        denominator = shots + xg
        
        if denominator == 0:
          return 0
        
        result = round(numerator / denominator, 4)
        return result

    def _duel_winning_ability(self, row):
        """
        Duel Winning Ability
        ((tackles_won + aerial_duels_won + take_ons_succeeded + challenges_won) / (tackles_att + aerial_duels_won + aerial_duels_lost + take_ons_attempted + challenges_att)) * (tackles_won + aerial_duels_won + take_ons_succeeded + challenges_won)
        Success rate across all types of 1v1 situations, weighted by volume
        """
        tackles_won = row['tackles_won']
        aerial_duels_won = row['aerial_duels_won']
        take_ons_succeeded = row['take_ons_succeeded']
        challenges_won = row['challenges_won']
        tackles_att = row['tackles_att']
        aerial_duels_lost = row['aerial_duels_lost']
        take_ons_attempted = row['take_ons_attempted']
        challenges_att = row['challenges_att']
        
        successes = tackles_won + aerial_duels_won + take_ons_succeeded + challenges_won
        attempts = tackles_att + aerial_duels_won + aerial_duels_lost + take_ons_attempted + challenges_att
        
        if attempts == 0:
            return 0
        
        success_rate = successes / attempts
        result = round(success_rate * successes, 4)
        return result

    def _tackling_score(self, row):
        t_won = row['tackles_won']
        t_att = row['tackles_att']
        if t_att == 0:
            return 0
        new_t_att = np.sqrt(t_att)
        rate = t_won/t_att

        final = rate * new_t_att
        return final
        


    def score_all_positions(self):
        position_groups = ['Forward', 'Midfielder', 'Defender']
        dfs = list()
        for group in position_groups:
          df = self.score_position_group(group)
          dfs.append(df)
        
        final = pd.concat(dfs, ignore_index=True)
        return final

