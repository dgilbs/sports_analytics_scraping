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
sum(fpms.minutes) as minutes,
sum(passes_completed) as passes_completed,
sum(passes_attempted) as passes_attempted, 
sum(total_pass_distance) as total_pass_distance,
sum(total_progressive_pass_distance) as total_progressive_pass_distance,
sum(short_passes_completed) as short_passes_completed,
sum(short_passes_attempted) as short_passes_attempted,
sum(medium_passes_completed) as medium_passes_completed,
sum(medium_passes_attempted) as medium_passed_attempted,
sum(long_passes_completed) as long_passes_completed,
sum(long_passes_attempted) as long_passes_attempted,
sum(assists) as assists,
sum(xag) as xag,
sum(xa) as xa,
sum(key_passes) as key_passes,
sum(passes_into_final_third) as passes_into_final_third,
sum(passes_into_penalty_area) as passes_into_penalty_area,
sum(crosses_into_penalty_area) as crosses_into_penalty_area,
sum(progressive_passes) as progressive_passes,
round((sum(passes_completed) * 1.0/sum(fpms.minutes)) * 90.0,3) as passes_completed_per_90,
round((sum(passes_attempted) * 1.0/sum(fpms.minutes)) * 90.0,3) as passes_attempted_per_90,
round((sum(total_pass_distance) * 1.0/sum(fpms.minutes)) * 90.0,3) as total_pass_distance_per_90,
round((sum(total_progressive_pass_distance) * 1.0/sum(fpms.minutes)) * 90.0,3) as total_progressive_pass_distance_per_90,
round((sum(short_passes_completed) * 1.0/sum(fpms.minutes)) * 90.0,3) as short_passes_completed_per_90,
round((sum(short_passes_attempted) * 1.0/sum(fpms.minutes)) * 90.0,3) as short_passes_attempted_per_90,
round((sum(medium_passes_completed) * 1.0/sum(fpms.minutes)) * 90.0,3) as medium_passes_completed_per_90,
round((sum(long_passes_completed) * 1.0/sum(fpms.minutes)) * 90.0,3) as long_passes_completed_per_90,
round((sum(long_passes_attempted) * 1.0/sum(fpms.minutes)) * 90.0,3) as long_passes_attempted_per_90,
round((sum(assists) * 1.0/sum(fpms.minutes)) * 90.0,3) as assists_per_90,
round((sum(xag) * 1.0/sum(fpms.minutes)) * 90.0,3) as xag_per_90,
round((sum(xa) * 1.0/sum(fpms.minutes)) * 90.0,3) as xa_per_90,
round((sum(key_passes) * 1.0/sum(fpms.minutes)) * 90.0,3) as key_passes_per_90,
round((sum(passes_into_final_third) * 1.0/sum(fpms.minutes)) * 90.0,3) as passes_into_final_third_per_90,
round((sum(passes_into_penalty_area) * 1.0/sum(fpms.minutes)) * 90.0,3) as passes_into_penalty_area_per_90,
round((sum(crosses_into_penalty_area) * 1.0/sum(fpms.minutes)) * 90.0,3) as crosses_into_penalty_area_per_90,
round((sum(progressive_passes) * 1.0/sum(fpms.minutes)) * 90.0,3) as progressive_passes_per_90
from f_player_match_passing fpms
left join dim_players dp 
on dp.id = fpms.player_id
left join dim_squads ds 
on ds.id = fpms.team_id
left join dim_team_matches dtm
on dtm.match_id = fpms.match_id and dtm.team_id = fpms.team_id
left join dim_squads dsa 
on dsa.id = dtm.opponent_id
left join dim_competitions dc
on dc.id = dtm.competition_id
left join dim_player_appearances dpa 
on dpa.id = fpms.id
group by 1,2,3,4