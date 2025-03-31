{{
    config(
        materialized="view",
    )
}}


select 
dp.player,
ds.squad,
dtm.season,
dc.competition,
dsr.playing_position as roster_position,
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
sum(ft.progressive_passes_recieved) as progressive_passes_recieved,
round((sum(touches) * 1.0/sum(ft.minutes)) * 90.0,3) as touches_per_90,
round((sum(touches_def_penalty_area) * 1.0/sum(ft.minutes)) * 90.0,3) as touches_def_penalty_area_per_90,
round((sum(touches_def_third) * 1.0/sum(ft.minutes)) * 90.0,3) as touches_def_third_per_90,
round((sum(touches_mid_third) * 1.0/sum(ft.minutes)) * 90.0,3) as touches_mid_third_per_90,
round((sum(touches_att_third) * 1.0/sum(ft.minutes)) * 90.0,3) as touches_att_third_per_90,
round((sum(touches_att_penalty_area) * 1.0/sum(ft.minutes)) * 90.0,3) as touches_att_penalty_area_per_90,
round((sum(touches_live) * 1.0/sum(ft.minutes)) * 90.0,3) as touches_live_per_90,
round((sum(take_ons_attempted) * 1.0/sum(ft.minutes)) * 90.0,3) as take_ons_attempted_per_90,
round((sum(take_ons_succeeded) * 1.0/sum(ft.minutes)) * 90.0,3) as take_ons_succeeded_per_90,
round((sum(take_ons_tackled) * 1.0/sum(ft.minutes)) * 90.0,3) as take_ons_tackled_per_90,
round((sum(carries) * 1.0/sum(ft.minutes)) * 90.0,3) as carries_per_90,
round((sum(total_carries_distance) * 1.0/sum(ft.minutes)) * 90.0,3) as total_carries_distance_per_90,
round((sum(total_progressive_carries_distance) * 1.0/sum(ft.minutes)) * 90.0,3) as total_progressive_carries_distance_per_90,
round((sum(progressive_carries) * 1.0/sum(ft.minutes)) * 90.0,3) as progressive_carries_per_90,
round((sum(carries_into_final_third) * 1.0/sum(ft.minutes)) * 90.0,3) as carries_into_final_third_per_90,
round((sum(carries_into_penalty_area) * 1.0/sum(ft.minutes)) * 90.0,3) as carries_into_penalty_area_per_90,
round((sum(carries_miscontrolled) * 1.0/sum(ft.minutes)) * 90.0,3) as carries_miscontrolled_per_90,
round((sum(carries_dispossessed) * 1.0/sum(ft.minutes)) * 90.0,3) as carries_dispossessed_per_90,
round((sum(passes_recieved) * 1.0/sum(ft.minutes)) * 90.0,3) as passes_recieved_per_90,
round((sum(progressive_passes_recieved) * 1.0/sum(ft.minutes)) * 90.0,3) as progressive_passes_recieved_per_90
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
left join soccer.dim_squad_rosters dsr 
on dsr.player_id = ft.player_id and ft.team_id = dsr.squad_id and cast(dsr.season as text) = dtm.season
group by 1,2,3,4,5