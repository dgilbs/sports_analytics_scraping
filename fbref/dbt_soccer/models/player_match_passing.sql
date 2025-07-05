{{
    config(
        materialized="view",
    )
}}


select 
dp.player,
fpms.id as appearance_id,
ds.squad,
dtm.match_date,
dsa.squad as opponent,
dtm.season,
dc.competition,
dsr.playing_position,
dpa."position" as match_position,
split_part(dpa."position", ',', 1) as primary_position,
case 
    when split_part(dpa."position", ',', 1) in ('LB', 'RB') then 'Defender'
    when split_part(dpa."position", ',', 1) in ('CB') then 'Defender'
    when split_part(dpa."position", ',', 1) in ('LM', 'CM', 'RM', 'DM', 'AM') then 'Midfielder'
    when split_part(dpa."position", ',', 1) in ('FW', 'LW', 'RW') then 'Forward'
end as position_group,
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
passes_completed,
passes_attempted, 
passes_completed::numeric/nullif(passes_attempted, 0)  pass_completion_rate,
total_pass_distance,
total_progressive_pass_distance,
short_passes_completed,
short_passes_attempted,
short_passes_completed/nullif(short_passes_attempted::numeric, 0) as short_pass_completion_rate,
medium_passes_completed,
medium_passes_attempted,
medium_passes_completed/nullif(medium_passes_attempted::numeric, 0) as medium_pass_completion_rate,
long_passes_completed,
long_passes_attempted,
long_passes_completed/nullif(long_passes_attempted::numeric, 0) as long_pass_completion_rate,
assists,
xag,
xa ,
key_passes ,
passes_into_final_third ,
passes_into_penalty_area ,
crosses_into_penalty_area ,
progressive_passes 
from soccer.f_player_match_passing fpms
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
on dsr.player_id = fpms.player_id and fpms.team_id = dsr.squad_id and cast(dsr.season as varchar(50)) = dtm.season
