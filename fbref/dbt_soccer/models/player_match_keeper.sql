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
ft.minutes,
ft.shots_on_target_against,
ft.goals_allowed,
ft.saves,
ft.psxg,
ft.passes_launched_completed,
ft.passes_launched_attempted,
ft.passes_attempted_total,
ft.throws_attempted,
ft.average_pass_length,
ft.goal_kicks_attempted,
ft.pct_goal_kicks_launched,
ft.pct_of_passes_launched,
ft.avg_goal_kick_length,
ft.crosses_faced,
ft.crosses_stopped,
ft.defensive_actions_outside_pen_area,
ft.avg_distance_of_defensive_actions
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