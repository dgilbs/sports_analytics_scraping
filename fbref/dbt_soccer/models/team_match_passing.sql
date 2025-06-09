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
sum(passes_completed) as passes_completed,
sum (passes_attempted) as passes_attempted, 
sum(total_pass_distance) as total_pass_distance,
sum(total_progressive_pass_distance) as total_progressive_pass_distance,
sum(short_passes_completed) as short_passes_completed,
sum(short_passes_attempted) as short_passes_attempted,
sum(medium_passes_completed) as medium_passes_completed,
sum(medium_passes_attempted) as medium_passes_attempted,
sum(long_passes_completed) as long_passes_completed,
sum(long_passes_attempted) as long_passes_attempted,
sum(assists) as assists,
sum(xag) as xag,
sum(xa) as xa,
sum(key_passes) as key_passes,
sum(passes_into_final_third) as passes_into_final_third,
sum(passes_into_penalty_area) as passes_into_penalty_area,
sum(crosses_into_penalty_area) as crosses_into_penalty_area,
sum(progressive_passes) progressive_passes
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
group by 1,2,3,4,5
