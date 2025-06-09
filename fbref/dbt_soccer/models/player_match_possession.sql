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
ft.minutes,
ft.touches,
ft.touches_def_penalty_area,
ft.touches_def_third,
ft.touches_mid_third,
ft.touches_att_third,
ft.touches_att_penalty_area,
ft.touches_live,
ft.take_ons_attempted,
ft.take_ons_succeeded,
ft.take_ons_tackled,
ft.carries,
ft.total_carries_distance,
ft.total_progressive_carries_distance,
ft.progressive_carries,
ft.carries_into_final_third,
ft.carries_into_penalty_area,
ft.carries_miscontrolled,
ft.carries_dispossessed,
ft.passes_recieved,
ft.progressive_passes_recieved 
from soccer.f_player_match_possession ft
left join soccer.dim_players dp 
on dp.id = ft.player_id
left join soccer.dim_squads ds 
on ds.id = ft.team_id
left join soccer.dim_team_matches dtm
on dtm.match_id = ft.match_id and dtm.team_id = ft.team_id
left join soccer.dim_squads dsa 
on dsa.id = dtm.opponent_id
left join soccer.dim_competitions dc
on dc.id = dtm.competition_id
left join soccer.dim_player_appearances dpa 
on dpa.id = ft.id