select 
dm.id,
dm.match_date,
dsh.squad as home_team,
dsa.squad as away_team
from 
soccer.dim_matches dm 
left join soccer.dim_squads dsh 
on dsh.id = dm.home_team_id 
left join soccer.dim_squads dsa 
on dsa.id = dm.away_team_id
where 
season = '2025'
and competition_id = 182
