-- Fantasy draft rankings: one row per player per season with scoring efficiency,
-- ceiling, floor, and consistency metrics to guide pick decisions.
with match_pts as (
    select
        player_id,
        player_name,
        draft_position,
        season,
        total_points,
        minutes_played,
        match_date
    from {{ ref('fantasy_match_points') }}
    where draft_position is not null
      and minutes_played > 0
),

season as (
    select * from {{ ref('fantasy_season_points') }}
    where draft_position is not null
),

per_match as (
    select
        player_id,
        player_name,
        draft_position,
        season,
        count(*)                                                        as matches_played,
        round(avg(total_points)::numeric, 2)                           as avg_pts_per_match,
        max(total_points)                                               as ceiling,
        min(total_points)                                               as floor,
        -- consistency: % of matches with pts > 0
        round(
            100.0 * sum(case when total_points > 0 then 1 else 0 end)
            / count(*), 1
        )                                                               as consistency_pct,
        -- boom rate: % of matches with pts >= 10
        round(
            100.0 * sum(case when total_points >= 10 then 1 else 0 end)
            / count(*), 1
        )                                                               as boom_rate_pct,
        -- bust rate: % of matches with pts < 0
        round(
            100.0 * sum(case when total_points < 0 then 1 else 0 end)
            / count(*), 1
        )                                                               as bust_rate_pct,
        -- standard deviation (lower = more consistent)
        round(stddev(total_points)::numeric, 2)                        as pts_stddev,
        max(match_date)                                                 as last_match
    from match_pts
    group by 1, 2, 3, 4
)

select
    pm.season,
    pm.draft_position,
    pm.player_name,
    pm.matches_played,
    s.total_points                                                      as season_total,
    pm.avg_pts_per_match,
    s.points_per_90,
    pm.ceiling,
    pm.floor,
    pm.consistency_pct,
    pm.boom_rate_pct,
    pm.bust_rate_pct,
    pm.pts_stddev,
    -- rank within position and season by avg pts per match
    rank() over (
        partition by pm.draft_position, pm.season
        order by pm.avg_pts_per_match desc
    )                                                                   as position_rank,
    pm.last_match
from per_match pm
join season s
    on pm.player_id = s.player_id
    and pm.draft_position = s.draft_position
    and pm.season = s.season
order by pm.season desc, pm.draft_position, position_rank
