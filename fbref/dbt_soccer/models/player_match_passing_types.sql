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
dpa."position",
fpms.minutes,
passes_attempted,
passes_live,
passes_dead_ball,
passes_crosses,
passes_throw_ins,
passes_switches,
corner_kicks_inswinging,
corner_kicks_outswinging, 
corner_kicks_straight, 
passes_completed,
passes_offside,
passes_blocked
from soccer.f_player_match_passing_types fpms
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