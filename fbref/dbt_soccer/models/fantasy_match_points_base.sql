select 
dp.id as player_id,
dp.player,
ds.squad,
dsa.squad as opponent,
dtm.season,
dtm.match_date,
dtm.match_id,
mw.wk as fantasy_week,
dc.competition,
dsr.playing_position as roster_position,
dpa."position" as match_postion,
fant.fantasy_position,
fpms.minutes,
dtm.goals_against as team_goals_conceded,
goals - pk_goals as goals,
fpms.assists,
coalesce(f_keep.saves, 0) as saves,
pk_goals,
pk_attempts - pk_goals as pk_misses,
f_misc.yellow_cards,
f_misc.second_yellow_cards,
f_misc.red_cards,
fpms.touches,
f_def.tackles_won,
fpms.blocks,
fpms.interceptions,
f_poss.take_ons_succeeded,
f_misc.own_goals,
cast(f_pass.passes_completed as float)/nullif(f_pass.passes_attempted, 0) as passing_pct,
(goals - pk_goals) * fp.goal_points as goal_points,
f_def.tackles_won * fp.tackles_won_points as tackles_won_points,
case 
    when dtm.goals_against = 0 then fp.clean_sheet_points
    else 0
end as clean_sheet_points,
dtm.goals_against * fp.goals_conceded_points as goals_conceded_points,
fpms.assists * fp.assist_points as assist_points,
case 
    when fpms.minutes >= 60 then fp.played_60_mins
    else 0
end as minutes_points,
fpms.interceptions * fp.interception_points as interception_points,
case 
    when fpms.minutes >= 1 then fp.appearance_points
    else 0
end as appearance_points,
case
    when f_misc.second_yellow_cards < 1 then f_misc.red_cards * red_card_points
    else 0 
end as red_card_points,
case 
    when f_misc.second_yellow_cards <= 1 then f_misc.yellow_cards * yellow_card_points
    else 0 
end as yellow_card_points,
pk_goals * fp.pk_converted_points as pk_points,
coalesce(f_keep.saves,0) * fp.save_points as save_points,
(pk_attempts - pk_goals) * fp.pk_missed_points as pk_missed_points,
case
    when cast(f_pass.passes_completed as float)/nullif(f_pass.passes_attempted, 0) >= 0.85 and fpms.minutes >= 60 then fp.pass_completion_points
    else 0
end as pass_completion_points,
case 
    when fpms.touches >= 60 then fp.touches_points
    else 0
end as touches_points,
fpms.blocks * fp.block_points as block_points,
fpms.goal_creating_actions * fp.gca_points as gca_points,
take_ons_succeeded * fp.take_on_points as take_on_points,
f_misc.own_goals * fp.own_goal_points as own_goal_points
from soccer.f_player_match_summary fpms
left join soccer.f_player_match_passing f_pass 
on f_pass.id = fpms.id
left join soccer.f_player_match_possession f_poss
on f_poss.id = fpms.id
left join soccer.f_player_match_keeper f_keep 
on f_keep.id = fpms.id
left join soccer.f_player_match_misc f_misc
on f_misc.id = fpms.id
left join soccer.f_player_match_defense f_def 
on f_def.id = fpms.id
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
left join soccer.dim_squad_rosters dsr 
on dsr.player_id = fpms.player_id and fpms.team_id = dsr.squad_id and cast(dsr.season as text) = dtm.season
left join soccer.nwsfl_fantasy_players_2025 fant on fant.player_id = dp.id
left join soccer.fantasy_points fp 
on fp.playing_position = fant.fantasy_position
left join soccer.nwsfl_match_weeks mw 
on mw.id = dtm.match_id
where dc.competition = 'NWSL' and dtm.season in ('2024', '2025')
