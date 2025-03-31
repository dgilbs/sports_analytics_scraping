{{
    config(
        materialized="view",
    )
}}


select 
sq.squad,
sqa.squad as opponent,
home_or_away,
dtm.season, 
comp.competition,
goals_scored,''
goals_against,
xg_for,
xg_against,
case 
	when goals_scored > goals_against then 'Win'
	when goals_scored < goals_against then 'Loss'
	else 'Tie'
end as match_result,
xg_for - xg_against as xg_diff
from soccer.dim_team_matches dtm
left join soccer.dim_squads sq 
on dtm.team_id = sq.id
left join soccer.dim_squads sqa 
on sqa.id = dtm.opponent_id
left join soccer.dim_competitions comp 
on dtm.competition_id = comp.id