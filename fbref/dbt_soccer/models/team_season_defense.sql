{{
    config(
        materialized="view",
    )
}}


select 
ds.squad,
dtm.season,
dc.competition,
sum(fpms.minutes) as minutes,
sum(tackles_att) as tackles_att,
sum(tackles_won) as tackles_won,
sum(tackles_def_third) as tackles_def_third,
sum(tackles_mid_third) as tackles_mid_third,
sum(tackles_att_third) as tackles_att_third,
sum(challenges_won) as challenges_won,
sum(challenges_att) as challenges_att,
sum(blocks) as blocks,
sum(shot_blocks) as shot_blocks,
sum(pass_blocks) as pass_blocks,
sum(interceptions) as interceptions,
sum(clearances) as clearances,
sum(errors_lead_to_shot) as errors_lead_to_shot
from soccer.f_player_match_defense fpms
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