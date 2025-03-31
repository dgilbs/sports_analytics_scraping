SELECT 
DISTINCT 
pss.player,
fant.playing_position,
fp.goal_points,
fp.tackles_won_points,
fp.clean_sheet_points,
fp.goals_conceded_points,
fp.save_points,
fp.penalty_save_points,
fp.assist_points,
fp.played_60_mins,
fp.interception_points,
fp.appearance_points,
fp.yellow_card_points,
fp.red_card_points,
fp.pk_converted_points,
fp.pk_missed_points,
fp.own_goal_points,
fp.pass_completion_points,
fp.touches_points,
fp.block_points,
fp.gca_points,
fp.take_on_points
FROM soccer.player_season_summary pss
LEFT JOIN soccer.nwsl_fantasy_players fant
ON pss.player = fant.player
LEFT JOIN soccer.fantasy_points fp 
ON fp.playing_position = fant.playing_position
WHERE competition = 'NWSL' AND season = '2024'