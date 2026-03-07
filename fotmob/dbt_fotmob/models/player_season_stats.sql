with base as (
    select * from {{ ref('player_match_stats') }}
    where minutes_played > 0
)
select
    player_id,
    player_name,
    season,
    team_id,
    team_name,
    position_group,
    count(distinct match_id)        as matches_played,
    sum(minutes_played)             as minutes_played,
    -- scoring
    sum(goals)                      as goals,
    sum(assists)                    as assists,
    sum(xg)                         as xg,
    sum(xa)                         as xa,
    sum(xg_plus_xa)                 as xg_plus_xa,
    sum(shots_on_target)            as shots_on_target,
    -- attacking
    sum(touches)                    as touches,
    sum(touches_in_opposition_box)  as touches_in_opposition_box,
    sum(chances_created)            as chances_created,
    sum(passes_into_final_third)    as passes_into_final_third,
    -- passing
    sum(accurate_passes_succeeded)  as accurate_passes,
    sum(accurate_passes_attempted)  as accurate_passes_attempted,
    {{ safe_divide_round('sum(accurate_passes_succeeded)', 'sum(accurate_passes_attempted)') }} as accurate_passes_pct,
    sum(accurate_crosses_succeeded) as accurate_crosses,
    sum(accurate_crosses_attempted) as accurate_crosses_attempted,
    {{ safe_divide_round('sum(accurate_crosses_succeeded)', 'sum(accurate_crosses_attempted)') }} as accurate_crosses_pct,
    sum(accurate_long_balls_succeeded) as accurate_long_balls,
    sum(accurate_long_balls_attempted) as accurate_long_balls_attempted,
    {{ safe_divide_round('sum(accurate_long_balls_succeeded)', 'sum(accurate_long_balls_attempted)') }} as accurate_long_balls_pct,
    -- dribbles
    sum(successful_dribbles_succeeded) as successful_dribbles,
    sum(successful_dribbles_attempted) as successful_dribbles_attempted,
    {{ safe_divide_round('sum(successful_dribbles_succeeded)', 'sum(successful_dribbles_attempted)') }} as successful_dribbles_pct,
    -- defense
    sum(tackles)                    as tackles,
    sum(interceptions)              as interceptions,
    sum(blocks)                     as blocks,
    sum(clearances)                 as clearances,
    sum(recoveries)                 as recoveries,
    sum(dribbled_past)              as dribbled_past,
    sum(defensive_contributions)    as defensive_contributions,
    -- duels
    sum(duels_won)                  as duels_won,
    sum(duels_lost)                 as duels_lost,
    {{ safe_divide_round('sum(duels_won)', 'sum(duels_won) + sum(duels_lost)') }} as duels_won_pct,
    sum(ground_duels_won_succeeded) as ground_duels_won,
    sum(ground_duels_won_attempted) as ground_duels_attempted,
    {{ safe_divide_round('sum(ground_duels_won_succeeded)', 'sum(ground_duels_won_attempted)') }} as ground_duels_pct,
    sum(aerial_duels_won_succeeded) as aerial_duels_won,
    sum(aerial_duels_won_attempted) as aerial_duels_attempted,
    {{ safe_divide_round('sum(aerial_duels_won_succeeded)', 'sum(aerial_duels_won_attempted)') }} as aerial_duels_pct,
    -- discipline
    sum(fouls_committed)            as fouls_committed,
    sum(was_fouled)                 as was_fouled,
    -- goalkeeper
    sum(saves)                      as saves,
    sum(goals_conceded)             as goals_conceded,
    sum(xgot_faced)                 as xgot_faced,
    sum(goals_prevented)            as goals_prevented,
    -- per-90 metrics
    {{ safe_divide_round('sum(goals)         * 90.0', 'sum(minutes_played)') }} as goals_per_90,
    {{ safe_divide_round('sum(assists)       * 90.0', 'sum(minutes_played)') }} as assists_per_90,
    {{ safe_divide_round('sum(xg)            * 90.0', 'sum(minutes_played)') }} as xg_per_90,
    {{ safe_divide_round('sum(xa)            * 90.0', 'sum(minutes_played)') }} as xa_per_90,
    {{ safe_divide_round('sum(xg_plus_xa)    * 90.0', 'sum(minutes_played)') }} as xg_plus_xa_per_90,
    {{ safe_divide_round('sum(chances_created) * 90.0', 'sum(minutes_played)') }} as chances_created_per_90,
    {{ safe_divide_round('sum(tackles)       * 90.0', 'sum(minutes_played)') }} as tackles_per_90,
    {{ safe_divide_round('sum(interceptions) * 90.0', 'sum(minutes_played)') }} as interceptions_per_90,
    {{ safe_divide_round('sum(recoveries)    * 90.0', 'sum(minutes_played)') }} as recoveries_per_90
from base
group by 1, 2, 3, 4, 5, 6
