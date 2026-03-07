with base as (
    select * from {{ ref('team_match_stats') }}
)
select
    team_id,
    team_name,
    season,
    count(distinct match_id)                        as matches_played,
    sum(goals)                                      as goals,
    sum(xg)                                         as xg,
    {{ safe_divide_round('sum(xg)', 'count(distinct match_id)', 3) }} as xg_per_match,
    sum(shots_on_target)                            as shots_on_target,
    sum(chances_created)                            as chances_created,
    -- passing
    sum(accurate_passes)                            as accurate_passes,
    sum(accurate_passes_attempted)                  as accurate_passes_attempted,
    {{ safe_divide_round('sum(accurate_passes)', 'sum(accurate_passes_attempted)') }} as accurate_passes_pct,
    sum(accurate_long_balls)                        as accurate_long_balls,
    sum(accurate_long_balls_attempted)              as accurate_long_balls_attempted,
    -- possession
    sum(touches)                                    as touches,
    sum(touches_in_opposition_box)                  as touches_in_opposition_box,
    sum(passes_into_final_third)                    as passes_into_final_third,
    -- defense
    sum(tackles)                                    as tackles,
    sum(interceptions)                              as interceptions,
    sum(blocks)                                     as blocks,
    sum(clearances)                                 as clearances,
    sum(recoveries)                                 as recoveries,
    sum(defensive_contributions)                    as defensive_contributions,
    -- duels
    sum(duels_won)                                  as duels_won,
    sum(duels_lost)                                 as duels_lost,
    {{ safe_divide_round('sum(duels_won)', 'sum(duels_won) + sum(duels_lost)') }} as duels_won_pct,
    -- discipline
    sum(fouls_committed)                            as fouls_committed,
    sum(was_fouled)                                 as was_fouled,
    round(avg(avg_player_rating)::numeric, 2)       as avg_player_rating
from base
group by 1, 2, 3
