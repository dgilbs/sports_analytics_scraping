{{
    config(
        materialized="table",
    )
}}



select 
fp.*,
fr.nwsfl_team_id,
fr.status,
ft.manager,
ft.team_name
from {{ ref('fantasy_match_points') }} fp 
left join soccer.nwsfl_match_weeks sch 
on sch.id = fp.match_id
left join soccer.fantasy_rosters fr
on fp.player_id = fr.player_id and fr.fantasy_week = sch.wk
left join soccer.fantasy_teams ft
on fr.nwsfl_team_id = ft.id