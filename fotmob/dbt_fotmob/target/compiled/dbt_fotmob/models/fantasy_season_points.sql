with base as (
    select * from "neondb"."fotmob"."fantasy_match_points"
    where draft_position is not null
)

select
    player_id,
    player_name,
    draft_position,
    season,
    count(distinct match_id)            as matches_played,
    sum(minutes_played)                 as minutes_played,

    -- total fantasy points
    round(cast(
        sum(pts_appearance)
        + sum(pts_60_minutes)
        + sum(pts_assists)
        + sum(pts_interceptions)
        + sum(pts_blocks)
        + sum(pts_goal_creating_actions)
        + sum(pts_successful_takeons)
        + sum(pts_touches)
        + sum(pts_pass_completion)
        + sum(pts_yellow_cards)
        + sum(pts_red_card)
        + sum(pts_penalty_converted)
        + sum(pts_penalty_missed)
        + sum(pts_own_goal)
        + sum(pts_goals)
        + sum(pts_tackles)
        + sum(pts_clean_sheet)
        + sum(pts_goals_conceded)
        + sum(pts_saves)
        + sum(pts_penalty_save)
    as numeric), 1)                     as total_points,

    -- points per 90
    round(cast(
        (
            sum(pts_appearance)
            + sum(pts_60_minutes)
            + sum(pts_assists)
            + sum(pts_interceptions)
            + sum(pts_blocks)
            + sum(pts_goal_creating_actions)
            + sum(pts_successful_takeons)
            + sum(pts_touches)
            + sum(pts_pass_completion)
            + sum(pts_goals)
            + sum(pts_tackles)
            + sum(pts_clean_sheet)
            + sum(pts_goals_conceded)
            + sum(pts_saves)
        ) * 90.0 / nullif(sum(minutes_played), 0)
    as numeric), 2)                     as points_per_90,

    -- component breakdowns
    sum(pts_appearance)                 as pts_appearance,
    sum(pts_60_minutes)                 as pts_60_minutes,
    sum(pts_goals)                      as pts_goals,
    sum(pts_assists)                    as pts_assists,
    sum(pts_clean_sheet)                as pts_clean_sheet,
    sum(pts_goals_conceded)             as pts_goals_conceded,
    sum(pts_saves)                      as pts_saves,
    sum(pts_tackles)                    as pts_tackles,
    sum(pts_interceptions)              as pts_interceptions,
    sum(pts_blocks)                     as pts_blocks,
    sum(pts_goal_creating_actions)      as pts_goal_creating_actions,
    sum(pts_successful_takeons)         as pts_successful_takeons,
    sum(pts_touches)                    as pts_touches,
    sum(pts_pass_completion)            as pts_pass_completion,
    sum(pts_penalty_save)               as pts_penalty_save,
    sum(pts_penalty_missed)             as pts_penalty_missed

from base
group by 1, 2, 3, 4
order by total_points desc