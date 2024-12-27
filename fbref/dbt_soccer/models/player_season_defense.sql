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
sum(errors_lead_to_shot) as errors_lead_to_shot,
round((sum(tackles_att) * 1.0/sum(fpms.minutes)) * 90.0,3) as tackles_att_per_90,
round((sum(tackles_won) * 1.0/sum(fpms.minutes)) * 90.0,3) as tackles_won_per_90,
round((sum(tackles_def_third) * 1.0/sum(fpms.minutes)) * 90.0,3) as tackles_def_third_per_90,
round((sum(tackles_mid_third) * 1.0/sum(fpms.minutes)) * 90.0,3) as tackles_mid_third_per_90,
round((sum(tackles_att_third) * 1.0/sum(fpms.minutes)) * 90.0,3) as tackles_att_third_per_90,
round((sum(challenges_won) * 1.0/sum(fpms.minutes)) * 90.0,3) as challenges_won_per_90,
round((sum(challenges_att) * 1.0/sum(fpms.minutes)) * 90.0,3) as challenges_att_per_90,
round((sum(blocks) * 1.0/sum(fpms.minutes)) * 90.0,3) as blocks_per_90,
round((sum(shot_blocks) * 1.0/sum(fpms.minutes)) * 90.0,3) as shot_blocks_per_90,
round((sum(pass_blocks) * 1.0/sum(fpms.minutes)) * 90.0,3) as pass_blocks_per_90,
round((sum(interceptions) * 1.0/sum(fpms.minutes)) * 90.0,3) as interceptions_per_90,
round((sum(clearances) * 1.0/sum(fpms.minutes)) * 90.0,3) as clearances_per_90,
round((sum(errors_lead_to_shot) * 1.0/sum(fpms.minutes)) * 90.0,3) as errors_lead_to_shot_per_90
from f_player_match_defense fpms
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