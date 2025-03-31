
{{
    config(
        materialized="view",
    )
}}


select 
dp.player,
dsr.playing_position as roster_position,
ds.squad,
dtm.season,
dc.competition,
sum(fpms.minutes) as minutes,
sum(goals) as goals,
sum(assists) as assists,
sum (pk_goals) as pk_goals,
sum(pk_attempts) as pk_attempts,
sum(pk_attempts) - sum(pk_goals) as pk_misses,
sum(shots) as shots,
sum(shots_on_target) as shots_on_target,
sum(yellow_cards) as yellow_cards,
sum(red_cards) as red_cards,
sum(touches) as touches,
sum(tackles) as tackles,
sum(interceptions) as interceptions,
sum(blocks) as blocks,
sum(xg) as xg,
sum(npxg) as npxg,
sum(xag) as xag,
sum(shot_creating_actions) as shot_creating_actions,
sum(goal_creating_actions) as goal_creating_actions,
round((sum(goals) * 1.0/sum(fpms.minutes)) * 90.0,3) as goals_per_90,
round((sum(assists) * 1.0/sum(fpms.minutes)) * 90.0,3) as assists_per_90,
round((sum(shots) * 1.0/sum(fpms.minutes)) * 90.0,3) as shots_per_90,
round((sum(shots_on_target) * 1.0/sum(fpms.minutes)) * 90.0,3) as shots_on_target_per_90,
round((sum(touches) * 1.0/sum(fpms.minutes)) * 90.0,3) as touches_per_90,
round((sum(tackles) * 1.0/sum(fpms.minutes)) * 90.0,3) as tackles_per_90,
round((sum(interceptions) * 1.0/sum(fpms.minutes)) * 90.0,3) as interceptions_per_90,
round((sum(blocks) * 1.0/sum(fpms.minutes)) * 90.0,3) as blocks_per_90,
round((sum(xg::numeric) * 1.0/sum(fpms.minutes)) * 90.0,3) as xg_per_90,
round((sum(npxg::numeric) * 1.0/sum(fpms.minutes)) * 90.0,3) as npxg_per_90,
round((sum(xag::numeric) * 1.0/sum(fpms.minutes)) * 90.0,3) as xag_per_90,
round((sum(shot_creating_actions) * 1.0/sum(fpms.minutes)) * 90.0,3) as shot_creating_actions_per_90,
round((sum(goal_creating_actions) * 1.0/sum(fpms.minutes)) * 90.0,3) as goal_creating_actions_per_90
from soccer.f_player_match_summary fpms
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
on dsr.player_id = fpms.player_id and fpms.team_id = dsr.squad_id and cast(dsr.season as text) = dtm.season
group by 1,2,3,4,5