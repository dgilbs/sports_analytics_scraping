select
    rank() over (order by sum(weekly_points) desc)  as rank,
    manager,
    count(distinct week)                            as weeks_played,
    sum(weekly_points)                              as season_points,
    round(avg(weekly_points)::numeric, 1)           as avg_weekly_points,
    max(weekly_points)                              as best_week,
    min(weekly_points)                              as worst_week
from {{ ref('fantasy_weekly_points_2026') }}
group by manager
order by season_points desc
