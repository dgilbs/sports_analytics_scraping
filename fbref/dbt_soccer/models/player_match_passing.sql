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
dc.competition,
dpa."position",
fpms.minutes,
passes_completed,
passes_attempted, 
total_pass_distance,
total_progressive_pass_distance,
short_passes_completed,
short_passes_attempted,
medium_passes_completed,
medium_passes_attempted,
long_passes_completed,
long_passes_attempted,
assists,
xag,
xa ,
key_passes ,
passes_into_final_third ,
passes_into_penalty_area ,
crosses_into_penalty_area ,
progressive_passes 
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