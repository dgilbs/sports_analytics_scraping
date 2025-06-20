{{
    config(
        materialized="view",
    )
}}


select 
ds.squad,
dtm.season,
dtm.match_date,
dsa.squad as opponent,
dc.competition,
sum(ft.minutes) as minutes,
sum(ft.touches) as touches,
sum(ft.touches_def_penalty_area) as touches_def_penalty_area,
sum(ft.touches_def_third) as touches_def_third,
sum(ft.touches_mid_third) as touches_mid_third,
sum(ft.touches_att_third) as touches_att_third,
sum(ft.touches_att_penalty_area) as touches_att_penalty_area,
sum(ft.touches_live) as touches_live,
sum(ft.take_ons_attempted) as take_ons_attempted,
sum(ft.take_ons_succeeded) as take_ons_succeeded,
sum(ft.take_ons_tackled)take_ons_tackled,
sum(ft.carries) carries,
sum(ft.total_carries_distance) total_carries_distance,
sum(ft.total_progressive_carries_distance) as total_progressive_carries_distance,
sum(ft.progressive_carries) as progressive_carries,
sum(ft.carries_into_final_third) as carries_into_final_third,
sum(ft.carries_into_penalty_area) as carries_into_penalty_area,
sum(ft.carries_miscontrolled) as carries_miscontrolled,
sum(ft.carries_dispossessed) as carries_dispossessed,
sum(ft.passes_recieved) as passes_recieved,
sum(ft.progressive_passes_recieved) as progressive_passes_recieved
from soccer.f_player_match_possession ft
left join soccer.dim_squads ds 
on ds.id = ft.team_id
left join soccer.dim_team_matches dtm
on dtm.match_id = ft.match_id and dtm.team_id = ft.team_id
left join soccer.dim_squads dsa 
on dsa.id = dtm.opponent_id
left join soccer.dim_competitions dc
on dc.id = dtm.competition_id
group by 1,2,3,4,5