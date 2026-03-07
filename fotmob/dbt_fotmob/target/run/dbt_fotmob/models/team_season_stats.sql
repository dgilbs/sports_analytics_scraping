
  create view "neondb"."fotmob"."team_season_stats__dbt_tmp"
    
    
  as (
    with base as (
    select * from "neondb"."fotmob"."team_match_stats"
)
select
    team_id,
    team_name,
    season,
    count(distinct match_id)                        as matches_played,
    sum(goals)                                      as goals,
    sum(xg)                                         as xg,
    
    case
        when count(distinct match_id) = 0 or count(distinct match_id) is null
        then null
        else round(
            cast(sum(xg) as decimal) / cast(count(distinct match_id) as decimal),
            3
        )
    end
 as xg_per_match,
    sum(shots_on_target)                            as shots_on_target,
    sum(chances_created)                            as chances_created,
    -- passing
    sum(accurate_passes)                            as accurate_passes,
    sum(accurate_passes_attempted)                  as accurate_passes_attempted,
    
    case
        when sum(accurate_passes_attempted) = 0 or sum(accurate_passes_attempted) is null
        then null
        else round(
            cast(sum(accurate_passes) as decimal) / cast(sum(accurate_passes_attempted) as decimal),
            4
        )
    end
 as accurate_passes_pct,
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
    
    case
        when sum(duels_won) + sum(duels_lost) = 0 or sum(duels_won) + sum(duels_lost) is null
        then null
        else round(
            cast(sum(duels_won) as decimal) / cast(sum(duels_won) + sum(duels_lost) as decimal),
            4
        )
    end
 as duels_won_pct,
    -- discipline
    sum(fouls_committed)                            as fouls_committed,
    sum(was_fouled)                                 as was_fouled,
    round(avg(avg_player_rating)::numeric, 2)       as avg_player_rating
from base
group by 1, 2, 3
  );