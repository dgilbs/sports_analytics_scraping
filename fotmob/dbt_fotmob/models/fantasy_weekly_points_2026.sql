select
    frmp.manager,
    ft.teamname,
    frmp.fantasy_week                                                        as week,
    count(distinct frmp.player_id) filter (where not frmp.is_benched)       as active_players,
    count(distinct frmp.player_id) filter (where frmp.is_benched)            as benched_players,
    round(cast(
        sum(case when not frmp.is_benched then frmp.total_points else 0 end)
    as numeric), 1)                                                          as weekly_points
from {{ ref('fantasy_roster_match_points_2026') }} frmp
join {{ ref('fantasy_teams') }} ft on ft.manager = frmp.manager
group by 1, 2, 3
order by frmp.manager, week
