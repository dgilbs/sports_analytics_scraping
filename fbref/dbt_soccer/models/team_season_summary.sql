
{{
    config(
        materialized="view",
    )
}}


select 
ds.squad,
dtm.season,
dc.competition,
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
sum(goals) - sum(xg) as xg_difference,
sum(cast(shots_on_target as float))/sum(shots) as shot_ratio,
sum(xg)/sum(shots_on_target) as xg_per_shot
from soccer.f_player_match_summary fpms
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
group by 1,2,3