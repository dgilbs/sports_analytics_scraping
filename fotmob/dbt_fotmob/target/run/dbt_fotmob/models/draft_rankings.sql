
  create view "neondb"."fotmob"."draft_rankings__dbt_tmp"
    
    
  as (
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
        match_date,
        goals,
        assists,
        tackles_won,
        pts_pass_completion,
        pts_touches
    from "neondb"."fotmob"."fantasy_match_points"
    where draft_position is not null
      and minutes_played > 0
),

season as (
    select * from "neondb"."fotmob"."fantasy_season_points"
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
        -- consistency: # of matches with pts > 2
        sum(case when total_points > 2 then 1 else 0 end)              as games_over_2pts,
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
        sum(goals)                                                      as total_goals,
        sum(assists)                                                    as total_assists,
        sum(tackles_won)                                               as total_tackles_won,
        sum(case when pts_pass_completion > 0 then 1 else 0 end)       as games_passing_bonus,
        sum(case when pts_touches > 0 then 1 else 0 end)               as games_touch_bonus,
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
    pm.games_over_2pts,
    pm.boom_rate_pct,
    pm.bust_rate_pct,
    pm.pts_stddev,
    pm.total_goals,
    pm.total_assists,
    pm.total_tackles_won,
    pm.games_passing_bonus,
    pm.games_touch_bonus,
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
  );