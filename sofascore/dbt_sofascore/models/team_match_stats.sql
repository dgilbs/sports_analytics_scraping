with base as (
    select * from {{ ref('player_match_stats') }}
)

select
    event_id,
    season,
    match_date,
    team,
    side,
    home_team,
    away_team,
    home_score,
    away_score,
    goals_conceded,
    result,
    case when result = 'win'  then 1 else 0 end as win,
    case when result = 'draw' then 1 else 0 end as draw,
    case when result = 'loss' then 1 else 0 end as loss,

    -- Squad
    count(*)                                        as players_used,
    sum(case when not substitute then 1 else 0 end) as starters,
    avg(rating)                                     as avg_player_rating,

    -- Goals & Attacking
    sum(goals)                                      as goals,
    sum(assists)                                    as key_passes_total,
    sum(total_shots)                                as total_shots,
    sum(shots_on_target)                            as shots_on_target,
    {{ safe_divide_round('sum(shots_on_target)', 'sum(total_shots)') }} as shot_accuracy,

    -- Passing
    sum(total_pass)                                 as total_passes,
    sum(accurate_pass)                              as accurate_passes,
    {{ safe_divide_round('sum(accurate_pass)', 'sum(total_pass)') }}    as pass_completion,
    sum(total_long_balls)                           as total_long_balls,
    sum(total_cross)                                as total_crosses,
    sum(accurate_cross)                             as accurate_crosses,

    -- Progression
    sum(carries_count)                              as carries,
    sum(progressive_carries_count)                  as progressive_carries,
    sum(total_progression)                          as total_progression,

    -- Defending
    sum(total_tackle)                               as total_tackles,
    sum(won_tackle)                                 as tackles_won,
    sum(interception_won)                           as interceptions,
    sum(total_clearance)                            as clearances,
    sum(ball_recovery)                              as recoveries,

    -- Duels
    sum(duel_won)                                   as duels_won,
    sum(duel_lost)                                  as duels_lost,
    sum(aerial_won)                                 as aerial_won,
    sum(aerial_lost)                                as aerial_lost,

    -- Discipline
    sum(fouls)                                      as fouls,
    sum(was_fouled)                                 as was_fouled

from base
group by
    event_id, season, match_date, team, side,
    home_team, away_team, home_score, away_score, goals_conceded, result
