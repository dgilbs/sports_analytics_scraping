-- Match-by-match consistency analysis for draft prep.
-- One row per player per season. Shows how reliable a player's scoring is
-- across different contexts: match result, home/away, and recent form.
with match_pts as (
    select
        p.player_id,
        p.player_name,
        p.draft_position,
        p.match_date,
        p.match_id,
        p.team_id,
        p.team_name,
        p.opponent_name,
        p.minutes_played,
        p.total_points,
        p.season,
        -- derive home/away from dim_matches
        case when p.team_id = m.home_team_id then 'home' else 'away' end as side,
        -- match result
        case
            when p.team_goals_scored > p.team_goals_conceded then 'W'
            when p.team_goals_scored = p.team_goals_conceded then 'D'
            else 'L'
        end                                                               as match_result,
        -- rolling last 5 matches within the season (ordered by date)
        row_number() over (
            partition by p.player_id, p.season
            order by p.match_date desc
        )                                                                 as match_recency
    from {{ ref('fantasy_match_points') }} p
    left join {{ source('fotmob', 'dim_matches') }} m on p.match_id = m.match_id
    where p.draft_position is not null
      and p.minutes_played > 0
),

-- per-player per-season aggregates split by context
player_splits as (
    select
        m.player_id,
        m.player_name,
        m.draft_position,
        m.season,

        -- overall
        count(*)                                                    as matches_played,
        round(avg(m.total_points)::numeric, 2)                     as avg_pts,

        -- recent form (last 5 matches of the season)
        round(avg(case when m.match_recency <= 5
            then m.total_points end)::numeric, 2)                  as avg_pts_last5,

        -- home vs away
        round(avg(case when m.side = 'home'
            then m.total_points end)::numeric, 2)                  as avg_pts_home,
        round(avg(case when m.side = 'away'
            then m.total_points end)::numeric, 2)                  as avg_pts_away,

        -- points by match result
        round(avg(case when m.match_result = 'W'
            then m.total_points end)::numeric, 2)                  as avg_pts_win,
        round(avg(case when m.match_result = 'D'
            then m.total_points end)::numeric, 2)                  as avg_pts_draw,
        round(avg(case when m.match_result = 'L'
            then m.total_points end)::numeric, 2)                  as avg_pts_loss,

        -- result counts
        sum(case when m.match_result = 'W' then 1 else 0 end)      as matches_won,
        sum(case when m.match_result = 'D' then 1 else 0 end)      as matches_drawn,
        sum(case when m.match_result = 'L' then 1 else 0 end)      as matches_lost,

        -- scoring bands
        sum(case when m.total_points >= 15  then 1 else 0 end)     as matches_15_plus,
        sum(case when m.total_points >= 10
                  and m.total_points < 15  then 1 else 0 end)      as matches_10_to_15,
        sum(case when m.total_points >= 5
                  and m.total_points < 10  then 1 else 0 end)      as matches_5_to_10,
        sum(case when m.total_points >= 0
                  and m.total_points < 5   then 1 else 0 end)      as matches_0_to_5,
        sum(case when m.total_points < 0   then 1 else 0 end)      as matches_negative,

        -- trend: recent form vs season avg (positive = improving)
        round((
            avg(case when m.match_recency <= 5 then m.total_points end) -
            avg(m.total_points)
        )::numeric, 2)                                             as form_trend

    from match_pts m
    group by 1, 2, 3, 4
)

select
    ps.season,
    ps.draft_position,
    ps.player_name,
    ps.matches_played,
    ps.avg_pts,
    ps.avg_pts_last5,
    ps.form_trend,
    ps.avg_pts_home,
    ps.avg_pts_away,
    round((ps.avg_pts_home - ps.avg_pts_away)::numeric, 2)         as home_away_diff,
    ps.avg_pts_win,
    ps.avg_pts_draw,
    ps.avg_pts_loss,
    ps.matches_won,
    ps.matches_drawn,
    ps.matches_lost,
    ps.matches_15_plus,
    ps.matches_10_to_15,
    ps.matches_5_to_10,
    ps.matches_0_to_5,
    ps.matches_negative,
    rank() over (
        partition by ps.draft_position, ps.season
        order by ps.avg_pts_last5 desc nulls last
    )                                                               as form_rank
from player_splits ps
order by ps.season, ps.draft_position, form_rank
