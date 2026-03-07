
  create view "neondb"."fotmob"."player_season_stats__dbt_tmp"
    
    
  as (
    with base as (
    select * from "neondb"."fotmob"."player_match_stats"
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
    
    case
        when sum(accurate_passes_attempted) = 0 or sum(accurate_passes_attempted) is null
        then null
        else round(
            cast(sum(accurate_passes_succeeded) as decimal) / cast(sum(accurate_passes_attempted) as decimal),
            4
        )
    end
 as accurate_passes_pct,
    sum(accurate_crosses_succeeded) as accurate_crosses,
    sum(accurate_crosses_attempted) as accurate_crosses_attempted,
    
    case
        when sum(accurate_crosses_attempted) = 0 or sum(accurate_crosses_attempted) is null
        then null
        else round(
            cast(sum(accurate_crosses_succeeded) as decimal) / cast(sum(accurate_crosses_attempted) as decimal),
            4
        )
    end
 as accurate_crosses_pct,
    sum(accurate_long_balls_succeeded) as accurate_long_balls,
    sum(accurate_long_balls_attempted) as accurate_long_balls_attempted,
    
    case
        when sum(accurate_long_balls_attempted) = 0 or sum(accurate_long_balls_attempted) is null
        then null
        else round(
            cast(sum(accurate_long_balls_succeeded) as decimal) / cast(sum(accurate_long_balls_attempted) as decimal),
            4
        )
    end
 as accurate_long_balls_pct,
    -- dribbles
    sum(successful_dribbles_succeeded) as successful_dribbles,
    sum(successful_dribbles_attempted) as successful_dribbles_attempted,
    
    case
        when sum(successful_dribbles_attempted) = 0 or sum(successful_dribbles_attempted) is null
        then null
        else round(
            cast(sum(successful_dribbles_succeeded) as decimal) / cast(sum(successful_dribbles_attempted) as decimal),
            4
        )
    end
 as successful_dribbles_pct,
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
    
    case
        when sum(duels_won) + sum(duels_lost) = 0 or sum(duels_won) + sum(duels_lost) is null
        then null
        else round(
            cast(sum(duels_won) as decimal) / cast(sum(duels_won) + sum(duels_lost) as decimal),
            4
        )
    end
 as duels_won_pct,
    sum(ground_duels_won_succeeded) as ground_duels_won,
    sum(ground_duels_won_attempted) as ground_duels_attempted,
    
    case
        when sum(ground_duels_won_attempted) = 0 or sum(ground_duels_won_attempted) is null
        then null
        else round(
            cast(sum(ground_duels_won_succeeded) as decimal) / cast(sum(ground_duels_won_attempted) as decimal),
            4
        )
    end
 as ground_duels_pct,
    sum(aerial_duels_won_succeeded) as aerial_duels_won,
    sum(aerial_duels_won_attempted) as aerial_duels_attempted,
    
    case
        when sum(aerial_duels_won_attempted) = 0 or sum(aerial_duels_won_attempted) is null
        then null
        else round(
            cast(sum(aerial_duels_won_succeeded) as decimal) / cast(sum(aerial_duels_won_attempted) as decimal),
            4
        )
    end
 as aerial_duels_pct,
    -- discipline
    sum(fouls_committed)            as fouls_committed,
    sum(was_fouled)                 as was_fouled,
    -- goalkeeper
    sum(saves)                      as saves,
    sum(goals_conceded)             as goals_conceded,
    sum(xgot_faced)                 as xgot_faced,
    sum(goals_prevented)            as goals_prevented,
    -- per-90 metrics
    
    case
        when sum(minutes_played) = 0 or sum(minutes_played) is null
        then null
        else round(
            cast(sum(goals)         * 90.0 as decimal) / cast(sum(minutes_played) as decimal),
            4
        )
    end
 as goals_per_90,
    
    case
        when sum(minutes_played) = 0 or sum(minutes_played) is null
        then null
        else round(
            cast(sum(assists)       * 90.0 as decimal) / cast(sum(minutes_played) as decimal),
            4
        )
    end
 as assists_per_90,
    
    case
        when sum(minutes_played) = 0 or sum(minutes_played) is null
        then null
        else round(
            cast(sum(xg)            * 90.0 as decimal) / cast(sum(minutes_played) as decimal),
            4
        )
    end
 as xg_per_90,
    
    case
        when sum(minutes_played) = 0 or sum(minutes_played) is null
        then null
        else round(
            cast(sum(xa)            * 90.0 as decimal) / cast(sum(minutes_played) as decimal),
            4
        )
    end
 as xa_per_90,
    
    case
        when sum(minutes_played) = 0 or sum(minutes_played) is null
        then null
        else round(
            cast(sum(xg_plus_xa)    * 90.0 as decimal) / cast(sum(minutes_played) as decimal),
            4
        )
    end
 as xg_plus_xa_per_90,
    
    case
        when sum(minutes_played) = 0 or sum(minutes_played) is null
        then null
        else round(
            cast(sum(chances_created) * 90.0 as decimal) / cast(sum(minutes_played) as decimal),
            4
        )
    end
 as chances_created_per_90,
    
    case
        when sum(minutes_played) = 0 or sum(minutes_played) is null
        then null
        else round(
            cast(sum(tackles)       * 90.0 as decimal) / cast(sum(minutes_played) as decimal),
            4
        )
    end
 as tackles_per_90,
    
    case
        when sum(minutes_played) = 0 or sum(minutes_played) is null
        then null
        else round(
            cast(sum(interceptions) * 90.0 as decimal) / cast(sum(minutes_played) as decimal),
            4
        )
    end
 as interceptions_per_90,
    
    case
        when sum(minutes_played) = 0 or sum(minutes_played) is null
        then null
        else round(
            cast(sum(recoveries)    * 90.0 as decimal) / cast(sum(minutes_played) as decimal),
            4
        )
    end
 as recoveries_per_90
from base
group by 1, 2, 3, 4, 5, 6
  );