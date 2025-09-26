import pandas as pd
import numpy as np

class PlayerRatings:

  def __init__(self, benchmark_data, scoring_data, min_benchmark_minutes=450, min_scoring_minutes=45):
    self.benchmark_data = benchmark_data[benchmark_data.minutes >= min_benchmark_minutes]
    self.scoring_data = scoring_data[scoring_data.minutes >= min_scoring_minutes]
    self.key_metrics = [
        'shot_creation_efficiency'
    ]

  def add_metrics(self):
    self.benchmark_data['shot_creation_efficiency'] = self.benchmark_data.apply(lambda row: self._shot_creation_effiency(row), axis=1)
    self.scoring_data['shot_creation_efficiency'] = self.scoring_data.apply(lambda row: self._shot_creation_effiency(row), axis=1)

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
    benchmark = self.benchmark_data[self.benchmark_data.position_group == position_group]
    scoring = self.scoring_data[self.scoring_data.position_group == position_group]
    score_cols = list()
    for i in self.key_metrics:
      arr = [x for x in benchmark[i] if not np.isnan(x) and not np.isinf(x)]
      high = np.percentile(arr, 95)
      low = np.percentile(arr, 5)
      score_col = i + '_score'
      scoring[score_col] = scoring.apply(lambda row: self._normalize_score(row[i], high, low), axis=1)
      score_cols.append(score_col)

    dim_cols = ['player', 'squad', 'opponent', 'season', 'match_date', 'competition',
       'position_group']
    cols = dim_cols + score_cols

    scoring = scoring[cols]


    return scoring

  def score_position_group_quantile(self, position_group, n_bins=10):
    benchmark = self.benchmark_data[self.benchmark_data.position_group == position_group]
    scoring = self.scoring_data[self.scoring_data.position_group == position_group].copy()
    score_cols = []

    for metric in self.key_metrics:
      # Remove NaN/inf for binning
      arr = benchmark[metric].replace([np.inf, -np.inf], np.nan).dropna()
      # Create quantile bins (labels 0 to n_bins)
      bins = pd.qcut(arr, q=n_bins, labels=False, duplicates='drop')
      # Get bin edges
      bin_edges = pd.qcut(arr, q=n_bins, retbins=True, duplicates='drop')[1]

      # Assign each scoring value to a bin
      scoring[metric + '_score'] = pd.cut(
        scoring[metric],
        bins=bin_edges,
        labels=False,
        include_lowest=True
      )
      # Scale to 0–10
      scoring[metric + '_score'] = scoring[metric + '_score'].fillna(0).astype(int)
      score_cols.append(metric + '_score')

    dim_cols = ['player', 'squad', 'opponent', 'season', 'match_date', 'competition', 'position_group']
    cols = dim_cols + score_cols
    return scoring[cols]


  def _normalize_score(self, value, high_val, low_val):
    value = np.clip(value, a_min=low_val, a_max=high_val)
    scaled = 10 * (value - low_val) / (high_val - low_val)
    return scaled






season = pd.read_csv('fbref/benchmark_data/season_overall.csv')
matches = pd.read_csv('fbref/benchmark_data/match_overall.csv')
season = season[season.minutes > 450]
matches = matches[matches.minutes > 45]
temp = PlayerRatings(season, matches)
temp.add_metrics()
x = temp.score_position_group('Forward')
print(x)