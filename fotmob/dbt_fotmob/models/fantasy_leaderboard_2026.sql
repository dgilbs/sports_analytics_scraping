with weekly as (
    select
        manager,
        teamname,
        count(distinct week)              as weeks_played,
        sum(weekly_points)                as season_points,
        round(avg(weekly_points)::numeric, 1) as avg_weekly_points,
        max(weekly_points)                as best_week,
        min(weekly_points)                as worst_week
    from {{ ref('fantasy_weekly_points_2026') }}
    group by manager, teamname
),

penalties as (
    select
        manager,
        coalesce(transfer_penalty, 0) as transfer_penalty
    from {{ ref('fantasy_transfer_penalties_2026') }}
),

player_stats as (
    select
        manager,
        count(distinct player_id) filter (where not is_benched and minutes_played > 0
            and fantasy_week != 3)  as distinct_players_played,
        max(total_points) filter (where not is_benched
            and fantasy_week != 3)  as best_single_game,
        min(total_points) filter (where not is_benched
            and fantasy_week != 3)  as worst_single_game,
        count(*) filter (where not is_benched and total_points <= 0
            and fantasy_week != 3)  as times_zero_or_negative
    from {{ ref('fantasy_roster_match_points_2026') }}
    group by manager
)

select
    rank() over (
        order by (weekly.season_points + coalesce(pen.transfer_penalty, 0)) desc
    )                                                   as rank,
    weekly.manager,
    weekly.teamname,
    coalesce(ps.distinct_players_played, 0)             as distinct_players_played,
    weekly.season_points                                as match_points,
    coalesce(pen.transfer_penalty, 0)                   as transfer_penalty,
    weekly.season_points + coalesce(pen.transfer_penalty, 0) as season_points,
    weekly.avg_weekly_points,
    weekly.best_week,
    weekly.worst_week,
    ps.best_single_game,
    ps.worst_single_game,
    coalesce(ps.times_zero_or_negative, 0)              as times_zero_or_negative
from weekly
left join penalties pen on pen.manager = weekly.manager
left join player_stats ps on ps.manager = weekly.manager
order by season_points desc
