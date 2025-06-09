{{
    config(
        materialized="view",
    )
}}

select 
dp.player,
ds.squad,
dsa.squad as opponent,
dtm.season,
dtm.match_date,
dc.competition,
dsr.playing_position as roster_position,
dpa."position" as match_position,
split_part(dpa."position", ',', 1) as primary_position,
CASE
when split_part(dpa."position", ',', 1) in ('FW', 'RW', 'LW') then true
else false
end as is_forward,
case 
when split_part(dpa."position", ',', 1) in ('AM', 'CM', 'RM', 'LM', 'DM', 'MF') then true
else false
end as is_midfielder,
case 
when split_part(dpa."position", ',', 1) in ('CB', 'WB', 'RB', 'LB') then true
else false
end as is_defender,
case 
when split_part(dpa."position", ',', 1) = 'GK' then true
else false
end as is_goalkeeper,
case 
when split_part(dpa."position", ',', 1) in ('RW', 'LW', 'WB', 'RB', 'LB') then true
else false
end as is_winger,
fpms.minutes,
goals,
fpms.assists,
pk_goals,
pk_attempts,
pk_attempts - pk_goals as pk_misses,
shots,
shots_on_target,
fpms.yellow_cards,
fpms.red_cards,
fpms.touches,
xg,
npxg,
fpms.xag,
shot_creating_actions,
goal_creating_actions,
f_pass.passes_completed,
f_pass.passes_attempted, 
total_pass_distance,
total_progressive_pass_distance,
short_passes_completed,
short_passes_attempted,
medium_passes_completed,
medium_passes_attempted,
long_passes_completed,
long_passes_attempted,
xa,
key_passes,
passes_into_final_third,
passes_into_penalty_area,
crosses_into_penalty_area,
progressive_passes,
touches_def_penalty_area,
touches_def_third,
touches_mid_third,
touches_att_third,
touches_att_penalty_area,
touches_live,
take_ons_attempted,
take_ons_succeeded,
take_ons_tackled,
carries,
total_carries_distance,
total_progressive_carries_distance,
progressive_carries,
carries_into_final_third,
carries_into_penalty_area,
carries_miscontrolled,
carries_dispossessed,
passes_recieved,
progressive_passes_recieved,
passes_live,
passes_dead_ball,
passes_crosses,
passes_throw_ins,
passes_switches,
corner_kicks_inswinging,
corner_kicks_outswinging, 
corner_kicks_straight, 
passes_offside,
passes_blocked,
tackles_att,
tackles_won,
tackles_def_third,
tackles_mid_third,
tackles_att_third,
challenges_won,
challenges_att,
f_def.blocks,
shot_blocks,
pass_blocks,
f_def.interceptions,
clearances,
errors_lead_to_shot,
fouled,
fouls,
second_yellow_cards,
offsides,
pks_won,
ball_recoveries,
aerial_duels_won,
aerial_duels_lost,
own_goals
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
left join soccer.dim_squad_rosters dsr 
on dsr.player_id = fpms.player_id and fpms.team_id = dsr.squad_id and cast(dsr.season as text) = dtm.season
left join soccer.f_player_match_passing f_pass 
on fpms.player_id = f_pass.player_id and fpms.match_id = f_pass.match_id
left join soccer.f_player_match_possession f_poss
on fpms.player_id = f_poss.player_id and fpms.match_id = f_poss.match_id
left join soccer.f_player_match_passing_types f_pt 
on fpms.player_id = f_pt.player_id and fpms.match_id = f_pt.match_id
left join soccer.f_player_match_defense f_def 
on fpms.player_id = f_def.player_id and fpms.match_id = f_def.match_id
left join soccer.f_player_match_misc f_misc
on fpms.player_id = f_misc.player_id and fpms.match_id = f_misc.match_id