select 
player,
player_id,
match_id,
squad,
opponent,
season,
match_date,
fantasy_position,
fantasy_week,
goal_points + assist_points 
+ tackles_won_points + clean_sheet_points 
+ goals_conceded_points + minutes_points
+ interception_points + appearance_points
+ pk_points + pk_missed_points
+ pass_completion_points + touches_points
+ block_points + gca_points + take_on_points
+ coalesce(save_points, 0) + yellow_card_points + red_card_points
+ own_goal_points
as total_points
from {{ref('fantasy_match_points_base')}}
