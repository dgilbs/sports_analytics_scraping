{{
    config(
        materialized="view",
    )
}}

SELECT
dp.player,
dt.team,
dto.team AS opponent,
fabs.game_id,
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
true_shooting_pct,
effective_field_goal_pct,
three_point_attempt_rate,
defensive_rebound_pct,
offensive_rebound_pct,
assist_pct,
steal_pct,
usage_rate,
offensive_rating,
defensive_rating,
round(minutes_played::numeric, 2) AS minutes_played
FROM basketball.f_advanced_box_score fabs
LEFT JOIN basketball.dim_players dp
ON dp.id = fabs.player_id
LEFT JOIN basketball.dim_teams dt
ON dt.id = fabs.team_id
LEFT JOIN basketball.dim_team_results dtr
ON dtr.game_id = fabs.game_id AND dtr.team_id = fabs.team_id
LEFT JOIN basketball.dim_teams dto 
ON dto.id = dtr.opponent_id 
left join basketball.dim_rosters dr 
on dr.player_id = fabs.player_id and dr.season = dtr.season and dt.id = dr.team_id