select 
fpms.id,
dp.player,
ds.squad,
dsa.squad as opponent,
dtm.season,
dtm.match_date,
dc.competition,
fantpl.playing_position,
fpms.minutes,
fpms.goals,
fpms.assists,
poss.touches,
fpms.pk_goals - fpms.pk_attempts AS pk_misses,
fpms.pk_goals,
fpms.yellow_cards,
fpms.red_cards,
fpms.interceptions,
fpms.blocks,
fpms.goal_creating_actions,
fk.saves,
dtm.goals_against,
CASE 
	WHEN dtm.goals_against = 0 THEN 1
	ELSE 0
END AS is_shutout,
CASE
	WHEN fpms.minutes > 0 THEN 1
	ELSE 0
END AS is_appearance,
CASE 
	WHEN fpms.minutes >= 60 THEN 1
	ELSE 0
END AS is_minutes_points,
CASE 
    WHEN fpms.touches >= 60 then 1
    else 0
end as is_touches_points,
fantpl.goal_points,
fantpl.tackles_won_points,
fantpl.clean_sheet_points,
fantpl.goals_conceded_points,
fantpl.save_points,
fantpl.penalty_save_points,
fantpl.assist_points,
fantpl.played_60_mins,
fantpl.interception_points,
fantpl.appearance_points,
fantpl.yellow_card_points,
fantpl.red_card_points,
fantpl.pk_converted_points,
fantpl.pk_missed_points,
fantpl.own_goal_points,
fantpl.pass_completion_points,
fantpl.touches_points,
fantpl.block_points,
fantpl.gca_points,
fantpl.take_on_points
from soccer.f_player_match_summary fpms
left join soccer.dim_players dp 
on dp.id = fpms.player_id
left join soccer.dim_squads ds 
on ds.id = fpms.team_id
left join soccer.dim_team_matches dtm
on dtm.match_id = fpms.match_id and dtm.team_id = fpms.team_id
left join soccer.dim_squads dsa 
on dsa.id = dtm.opponent_id
left join soccer.dim_competitions dc
on dc.id = dtm.competition_id
left join soccer.dim_player_appearances dpa 
on dpa.id = fpms.id
LEFT JOIN soccer.f_player_match_possession poss
ON poss.id = dpa.id
LEFT JOIN soccer.f_player_match_keeper fk 
ON fk.id = dpa.id
JOIN soccer.nwsl_fantasy_2024_players fantpl 
ON fantpl.player = dp.player
WHERE competition = 'NWSL' AND season = '2024'