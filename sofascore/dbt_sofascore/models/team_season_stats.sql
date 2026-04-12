with base as (
    select * from {{ ref('team_match_stats') }}
)

select
    team,
    season,

    count(*)                                                        as matches_played,
    sum(win)                                                        as wins,
    sum(draw)                                                       as draws,
    sum(loss)                                                       as losses,
    sum(goals)                                                      as goals_for,
    sum(goals_conceded)                                             as goals_against,
    sum(goals) - sum(goals_conceded)                                as goal_difference,

    -- Shooting
    sum(total_shots)                                                as total_shots,
    sum(shots_on_target)                                            as shots_on_target,
    {{ safe_divide_round('sum(shots_on_target)', 'sum(total_shots)') }}    as shot_accuracy,
    {{ safe_divide_round('sum(goals)', 'sum(total_shots)') }}              as shot_conversion,

    -- Passing
    sum(total_passes)                                               as total_passes,
    sum(accurate_passes)                                            as accurate_passes,
    {{ safe_divide_round('sum(accurate_passes)', 'sum(total_passes)') }}   as pass_completion,
    sum(total_crosses)                                              as total_crosses,
    sum(accurate_crosses)                                           as accurate_crosses,

    -- Progression
    sum(carries)                                                    as carries,
    sum(progressive_carries)                                        as progressive_carries,

    -- Defending
    sum(total_tackles)                                              as total_tackles,
    sum(tackles_won)                                                as tackles_won,
    sum(interceptions)                                              as interceptions,
    sum(clearances)                                                 as clearances,
    sum(recoveries)                                                 as recoveries,
    {{ safe_divide_round('sum(tackles_won)', 'sum(total_tackles)') }}      as tackle_success_rate,

    -- Duels
    sum(duels_won)                                                  as duels_won,
    sum(duels_lost)                                                 as duels_lost,
    sum(aerial_won)                                                 as aerial_won,
    sum(aerial_lost)                                                as aerial_lost,
    {{ safe_divide_round('sum(aerial_won)', 'sum(aerial_won + aerial_lost)') }} as aerial_win_rate,

    -- Discipline
    sum(fouls)                                                      as fouls,
    avg(avg_player_rating)                                          as avg_player_rating

from base
group by team, season
