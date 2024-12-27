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
sum(passes_attempted) as passes_attempted,
sum(passes_live) as passes_live,
sum(passes_dead_ball) as passes_dead_ball,
sum(passes_crosses) as passes_crosses,
sum(passes_throw_ins) as passes_throw_ins,
sum(passes_switches) as passes_switches,
sum(corner_kicks_inswinging) as corner_kicks_inswinging,
sum(corner_kicks_outswinging) as corner_kicks_outswinging, 
sum(corner_kicks_straight) as corner_kicks_straight, 
sum(passes_completed) as passes_completed,
sum(passes_offside) as passes_offside,
sum(passes_blocked) as passes_blocked,
round((sum(passes_attempted) * 1.0/sum(fpms.minutes)) * 90.0,3) as passes_attempted_per_90,
round((sum(passes_live) * 1.0/sum(fpms.minutes)) * 90.0,3) as passes_live_per_90,
round((sum(passes_dead_ball) * 1.0/sum(fpms.minutes)) * 90.0,3) as passes_dead_ball_per_90,
round((sum(passes_crosses) * 1.0/sum(fpms.minutes)) * 90.0,3) as passes_crosses_per_90,
round((sum(passes_switches) * 1.0/sum(fpms.minutes)) * 90.0,3) as passes_switches_per_90,
round((sum(passes_completed) * 1.0/sum(fpms.minutes)) * 90.0,3) as passes_completed_per_90,
round((sum(passes_offside) * 1.0/sum(fpms.minutes)) * 90.0,3) as passes_offside_per_90,
round((sum(passes_blocked) * 1.0/sum(fpms.minutes)) * 90.0,3) as passes_blocked_per_90
from f_player_match_passing_types fpms
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