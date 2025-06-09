{{
    config(
        materialized="view",
    )
}}

SELECT
dp.player,
dt.team,
dto.team AS opponent,
fbbs.game_id,
dtr.is_playoffs,
dtr.is_commissioners_cup,
dtr.game_date,
dtr.season,
dr.playing_position,
case 
when dr.playing_position in ('C', 'F-C', 'C-F') then true 
else false
end as is_center,
case 
when dr.playing_position in  ('F', 'G-F', 'F-G', 'F-C', 'C-F') then true 
else false 
end as is_forward,
case 
when dr.playing_position in ('G', 'G-F', 'F-G') then true 
else false
end as is_guard,
case 
when dr.playing_position in ('G', 'G-F', 'F-G') then true 
else false
end as is_perimeter,
case 
when dr.playing_position in ('F', 'F-C', 'C-F', 'C') then true 
else false
end as is_post,
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
sum(pts) AS points, 
sum(plus_minus) AS plus_minus,
round(sum(minutes_played_int)::numeric, 2) AS minutes_played
FROM basketball.f_basic_box_score fbbs
LEFT JOIN basketball.dim_players dp
ON dp.id = fbbs.player_id
LEFT JOIN basketball.dim_teams dt
ON dt.id = fbbs.team_id
LEFT JOIN basketball.dim_team_results dtr
ON dtr.game_id = fbbs.game_id AND dtr.team_id = fbbs.team_id
LEFT JOIN basketball.dim_teams dto 
ON dto.id = dtr.opponent_id 
left join basketball.dim_rosters dr 
on dr.player_id = fbbs.player_id and dr.season = dtr.season and dt.id = dr.team_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16