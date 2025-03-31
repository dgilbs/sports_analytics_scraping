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
sum(ft.minutes) as minutes,
sum(ft.shots_on_target_against) as shots_on_target_against,
sum(ft.goals_allowed) as goals_allowed,
sum(ft.saves) as saves,
sum(ft.psxg) psxg,
sum(ft.passes_launched_completed) as passes_launched_completed,
sum(ft.passes_launched_attempted) as passes_launched_attempted,
sum(ft.passes_attempted_total) as passes_attempted_total,
sum(ft.throws_attempted) as throws_attempted,
sum(ft.goal_kicks_attempted) as goal_kicks_attempted,
sum(ft.crosses_faced) as crosses_faced,
sum(ft.crosses_stopped) as crosses_stopped,
sum(ft.defensive_actions_outside_pen_area) as defensive_actions_outside_pen_area,
round((sum(shots_on_target_against) * 1.0/sum(fpms.minutes)) * 90.0,3) as shots_on_target_against_per_90,
round((sum(goals_allowed) * 1.0/sum(fpms.minutes)) * 90.0,3) as goals_allowed_per_90,
round((sum(saves) * 1.0/sum(fpms.minutes)) * 90.0,3) as saves_per_90,
round((sum(psxg) * 1.0/sum(fpms.minutes)) * 90.0,3) as psxg_per_90,
round((sum(crosses_stopped) * 1.0/sum(fpms.minutes)) * 90.0,3) as crosses_stopped_per_90,
round((sum(defensive_actions_outside_pen_area) * 1.0/sum(fpms.minutes)) * 90.0,3) as defensive_actions_outside_pen_area_per_90
from soccer.f_player_match_keeper ft
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