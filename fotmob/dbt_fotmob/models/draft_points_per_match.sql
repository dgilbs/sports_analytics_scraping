-- Per-match fantasy points with rolling averages and cumulative trends.
-- One row per player per match per season. Useful for charting scoring patterns
-- and identifying players who are heating up or cooling down.
with base as (
    select
        player_id,
        player_name,
        draft_position,
        match_id,
        match_date,
        season,
        team_name,
        opponent_name,
        minutes_played,
        total_points,
        -- all point components
        pts_appearance,
        pts_60_minutes,
        pts_goals,
        pts_assists,
        pts_clean_sheet,
        pts_saves,
        pts_tackles,
        pts_interceptions,
        pts_blocks,
        pts_successful_takeons,
        pts_touches,
        pts_pass_completion,
        pts_penalty_save,
        pts_penalty_converted,
        pts_yellow_cards,
        pts_red_card,
        pts_goals_conceded,
        pts_penalty_missed,
        pts_own_goal
    from {{ ref('fantasy_match_points') }}
    where draft_position is not null
      and minutes_played > 0
),

ranked as (
    select
        *,
        row_number() over (
            partition by player_id, season
            order by match_date
        ) as match_number
    from base
)

select
    r.season,
    r.draft_position,
    r.player_name,
    r.match_number,
    r.match_date,
    r.team_name,
    r.opponent_name,
    r.minutes_played,
    r.total_points,

    -- rolling 5-match average within season (current match included)
    round(avg(r.total_points) over (
        partition by r.player_id, r.season
        order by r.match_date
        rows between 4 preceding and current row
    )::numeric, 2)                                          as rolling_5_avg,

    -- cumulative season average to date
    round(avg(r.total_points) over (
        partition by r.player_id, r.season
        order by r.match_date
        rows between unbounded preceding and current row
    )::numeric, 2)                                          as cumulative_avg,

    -- all point components
    r.pts_appearance,
    r.pts_60_minutes,
    r.pts_goals,
    r.pts_assists,
    r.pts_clean_sheet,
    r.pts_saves,
    r.pts_tackles,
    r.pts_interceptions,
    r.pts_blocks,
    r.pts_successful_takeons,
    r.pts_touches,
    r.pts_pass_completion,
    r.pts_penalty_save,
    r.pts_penalty_converted,
    r.pts_yellow_cards,
    r.pts_red_card,
    r.pts_goals_conceded,
    r.pts_penalty_missed,
    r.pts_own_goal

from ranked r
order by r.season, r.draft_position, r.player_name, r.match_number
