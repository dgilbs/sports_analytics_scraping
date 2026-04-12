with base as (
    select * from {{ ref('player_match_stats') }}
    where minutes_played > 0
)

select
    player_id,
    player_name,
    season,
    team,
    position,

    count(*)                                as matches_played,
    sum(minutes_played)                     as total_minutes,
    avg(rating)                             as avg_rating,
    avg(rating_alternative)                 as avg_rating_alternative,

    -- Goals & Attacking
    sum(goals)                              as goals,
    sum(assists)                            as assists,
    sum(key_passes)                         as key_passes,
    sum(total_shots)                        as total_shots,
    sum(shots_on_target)                    as shots_on_target,
    sum(big_chance_missed)                  as big_chances_missed,
    {{ safe_divide_round('sum(shots_on_target)', 'sum(total_shots)') }}                    as shot_accuracy,
    {{ safe_divide_round('sum(goals)', 'sum(total_shots)') }}                              as shot_conversion,

    -- Passing
    sum(total_pass)                         as total_passes,
    sum(accurate_pass)                      as accurate_passes,
    sum(total_long_balls)                   as total_long_balls,
    sum(accurate_long_balls)                as accurate_long_balls,
    sum(total_cross)                        as total_crosses,
    sum(accurate_cross)                     as accurate_crosses,
    {{ safe_divide_round('sum(accurate_pass)', 'sum(total_pass)') }}                       as pass_completion,
    {{ safe_divide_round('sum(accurate_long_balls)', 'sum(total_long_balls)') }}           as long_ball_completion,
    {{ safe_divide_round('sum(accurate_cross)', 'sum(total_cross)') }}                     as cross_completion,

    -- Carries & Progression
    sum(carries_count)                      as carries,
    sum(progressive_carries_count)          as progressive_carries,
    sum(total_progression)                  as total_progression,
    {{ safe_divide_round('sum(progressive_carries_count)', 'sum(carries_count)') }}        as progressive_carry_rate,

    -- Touches & Possession
    sum(touches)                            as touches,
    sum(possession_lost)                    as possession_lost,

    -- Duels
    sum(duel_won)                           as duels_won,
    sum(duel_lost)                          as duels_lost,
    sum(aerial_won)                         as aerial_won,
    sum(aerial_lost)                        as aerial_lost,
    {{ safe_divide_round('sum(duel_won)', 'sum(duel_won + duel_lost)') }}                  as duel_win_rate,
    {{ safe_divide_round('sum(aerial_won)', 'sum(aerial_won + aerial_lost)') }}            as aerial_win_rate,

    -- Defending
    sum(total_tackle)                       as total_tackles,
    sum(won_tackle)                         as tackles_won,
    sum(interception_won)                   as interceptions,
    sum(total_clearance)                    as clearances,
    sum(ball_recovery)                      as recoveries,
    {{ safe_divide_round('sum(won_tackle)', 'sum(total_tackle)') }}                        as tackle_success_rate,

    -- Discipline
    sum(fouls)                              as fouls,
    sum(was_fouled)                         as was_fouled,

    -- Per 90
    {{ safe_divide_round('sum(goals)',            'sum(minutes_played) / 90.0', 2) }}      as goals_per90,
    {{ safe_divide_round('sum(assists)',          'sum(minutes_played) / 90.0', 2) }}      as assists_per90,
    {{ safe_divide_round('sum(key_passes)',       'sum(minutes_played) / 90.0', 2) }}      as key_passes_per90,
    {{ safe_divide_round('sum(total_shots)',      'sum(minutes_played) / 90.0', 2) }}      as shots_per90,
    {{ safe_divide_round('sum(interception_won)', 'sum(minutes_played) / 90.0', 2) }}      as interceptions_per90,
    {{ safe_divide_round('sum(total_tackle)',     'sum(minutes_played) / 90.0', 2) }}      as tackles_per90,
    {{ safe_divide_round('sum(ball_recovery)',    'sum(minutes_played) / 90.0', 2) }}      as recoveries_per90,
    {{ safe_divide_round('sum(carries_count)',    'sum(minutes_played) / 90.0', 2) }}      as carries_per90

from base
group by player_id, player_name, season, team, position
