-- Position-specific underlying stats to complement fantasy point rankings.
-- One row per player per season. Helps evaluate WHY a player scores well
-- and spot breakout candidates.
with base as (
    select * from "neondb"."fotmob"."fantasy_match_points"
    where draft_position is not null
      and minutes_played > 0
),

agg as (
    select
        player_id,
        player_name,
        draft_position,
        season,
        count(*)                                                            as matches_played,
        sum(minutes_played)                                                 as total_minutes,

        -- ── UNIVERSAL ────────────────────────────────────────────────────────
        sum(coalesce(pts_goals, 0) /
            case draft_position
                when 'FW' then 4.0 when 'MF' then 5.0
                when 'DF' then 6.0 when 'GK' then 10.0 else 1 end
        )                                                                   as goals,
        sum(coalesce(pts_assists, 0) / 2.0)                                as assists,
        round(avg(case when clean_sheet then 1.0 else 0.0 end) * 100, 1)  as clean_sheet_pct,

        -- ── GK ───────────────────────────────────────────────────────────────
        round(avg(case when draft_position = 'GK'
            then coalesce(pts_saves / 0.5, 0) end)::numeric, 2)           as avg_saves,
        round(sum(case when draft_position = 'GK'
            then coalesce(pts_saves / 0.5, 0) else 0 end)
            / nullif(sum(minutes_played), 0) * 90, 2)                     as saves_per_90,

        -- ── DEFENDER ─────────────────────────────────────────────────────────
        round(avg(coalesce(pts_tackles /
            case draft_position when 'FW' then 1.0 when 'DF' then 0.5
            when 'MF' then 0.5 else null end, 0))::numeric, 2)            as avg_tackles,
        round(avg(coalesce(pts_interceptions / 0.5, 0))::numeric, 2)      as avg_interceptions,
        round(avg(coalesce(pts_blocks / 0.5, 0))::numeric, 2)             as avg_blocks,

        -- ── MIDFIELDER / FORWARD ─────────────────────────────────────────────
        round(avg(coalesce(pts_goal_creating_actions / 0.5, 0))::numeric, 2) as avg_chances_created,
        round(avg(coalesce(pts_successful_takeons / 0.5, 0))::numeric, 2) as avg_successful_takeons,
        round(avg(coalesce(pts_assists / 2.0, 0))::numeric, 2)            as avg_assists,

        -- pass completion bonus hit rate
        round(100.0 * sum(case when pts_pass_completion > 0 then 1 else 0 end)
            / count(*), 1)                                                 as pass_bonus_pct,

        -- touches bonus hit rate
        round(100.0 * sum(case when pts_touches > 0 then 1 else 0 end)
            / count(*), 1)                                                 as touches_bonus_pct

    from base
    group by 1, 2, 3, 4
)

select
    a.season,
    a.draft_position,
    a.player_name,
    a.matches_played,
    round((a.total_minutes::numeric / a.matches_played), 0)     as avg_minutes,
    a.goals,
    a.assists,
    a.clean_sheet_pct,
    -- position-relevant columns
    case when a.draft_position = 'GK' then a.avg_saves        end as avg_saves,
    case when a.draft_position = 'GK' then a.saves_per_90     end as saves_per_90,
    case when a.draft_position in ('DF','MF','FW')
         then a.avg_tackles                                    end as avg_tackles,
    case when a.draft_position in ('DF','MF','FW')
         then a.avg_interceptions                              end as avg_interceptions,
    case when a.draft_position in ('DF','MF','FW')
         then a.avg_blocks                                     end as avg_blocks,
    case when a.draft_position in ('MF','FW')
         then a.avg_chances_created                            end as avg_chances_created,
    case when a.draft_position in ('MF','FW')
         then a.avg_successful_takeons                         end as avg_takeons,
    a.pass_bonus_pct,
    a.touches_bonus_pct,
    rank() over (
        partition by a.draft_position, a.season
        order by a.goals + a.assists desc
    )                                                               as goal_contributions_rank
from agg a
order by a.season desc, a.draft_position, goal_contributions_rank