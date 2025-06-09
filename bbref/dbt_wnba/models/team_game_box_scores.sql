{{
    config(
        materialized="view",
    )
}}

SELECT
dt.team,
dto.team AS opponent,
fbbs.game_id,
dtr.is_playoffs,
dtr.is_commissioners_cup,
dtr.game_date,
dtr.season,
CASE 
	WHEN dtr.team_points > dtr.opponent_points THEN 'Win'
	ELSE 'Loss'
END AS game_result,
dtr.home_or_away,
sum(fg) AS field_goals,
sum(fga) AS field_goal_attempts,
sum(fg_3) AS field_goals_threes,
sum(fga_3) AS field_goal_threes_attempts,
sum(ft) AS free_throws,
sum(fta) AS free_throw_attempts,
sum(orb) AS offensive_rebounds,
sum(trb) - sum(orb) as defensive_rebounds,
sum(trb) AS total_rebounds,
sum(ast) AS assists, 
sum(stl) AS steals,
sum(blk) AS blocks,
sum(tov) AS turnovers,
sum(pf) AS personal_fouls,
sum(pts) AS points
FROM basketball.f_basic_box_score fbbs
LEFT JOIN basketball.dim_players dp
ON dp.id = fbbs.player_id
LEFT JOIN basketball.dim_teams dt
ON dt.id = fbbs.team_id
LEFT JOIN basketball.dim_team_results dtr
ON dtr.game_id = fbbs.game_id AND dtr.team_id = fbbs.team_id
LEFT JOIN basketball.dim_teams dto 
ON dto.id = dtr.opponent_id 
GROUP BY 1,2,3,4,5,6,7,8,9