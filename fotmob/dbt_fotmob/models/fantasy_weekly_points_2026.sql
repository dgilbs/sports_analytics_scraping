select
    manager,
    fantasy_week                                                        as week,
    count(distinct player_id) filter (where not is_benched)            as active_players,
    count(distinct player_id) filter (where is_benched)                as benched_players,
    round(cast(
        sum(case when not is_benched then total_points else 0 end)
    as numeric), 1)                                                     as weekly_points
from {{ ref('fantasy_roster_match_points_2026') }}
group by 1, 2
order by manager, week
