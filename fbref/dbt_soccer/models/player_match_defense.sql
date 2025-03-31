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
tackles_att,
tackles_won,
tackles_def_third,
tackles_mid_third,
tackles_att_third,
challenges_won,
challenges_att,
blocks,
shot_blocks,
pass_blocks,
interceptions,
clearances,
errors_lead_to_shot
from soccer.f_player_match_defense fpms
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