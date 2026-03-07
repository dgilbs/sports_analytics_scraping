with base as (
    select * from {{ ref('player_match_stats') }}
    where team_id is not null
),
matches as (
    select * from {{ source('fotmob', 'dim_matches') }}
),
teams as (
    select * from {{ source('fotmob', 'dim_teams') }}
)
select
    b.match_id,
    b.match_date,
    b.season,
    b.team_id,
    b.team_name,
    b.side,
    b.formation,
    -- opponent
    case when b.side = 'home' then m.away_team_id else m.home_team_id end as opponent_id,
    case when b.side = 'home' then ot.team_name   else ht.team_name   end as opponent_name,
    -- squad
    count(distinct case when b.bucket = 'starters' then b.player_id end) as starters_used,
    count(distinct b.player_id)             as players_used,
    -- scoring
    sum(b.goals)                            as goals,
    sum(b.xg)                               as xg,
    sum(b.shots_on_target)                  as shots_on_target,
    sum(b.chances_created)                  as chances_created,
    -- possession / passing
    sum(b.touches)                          as touches,
    sum(b.touches_in_opposition_box)        as touches_in_opposition_box,
    sum(b.passes_into_final_third)          as passes_into_final_third,
    sum(b.accurate_passes_succeeded)        as accurate_passes,
    sum(b.accurate_passes_attempted)        as accurate_passes_attempted,
    {{ safe_divide_round('sum(b.accurate_passes_succeeded)', 'sum(b.accurate_passes_attempted)') }} as accurate_passes_pct,
    sum(b.accurate_long_balls_succeeded)    as accurate_long_balls,
    sum(b.accurate_long_balls_attempted)    as accurate_long_balls_attempted,
    -- defense
    sum(b.tackles)                          as tackles,
    sum(b.interceptions)                    as interceptions,
    sum(b.blocks)                           as blocks,
    sum(b.clearances)                       as clearances,
    sum(b.recoveries)                       as recoveries,
    sum(b.defensive_contributions)          as defensive_contributions,
    -- duels
    sum(b.duels_won)                        as duels_won,
    sum(b.duels_lost)                       as duels_lost,
    {{ safe_divide_round('sum(b.duels_won)', 'sum(b.duels_won) + sum(b.duels_lost)') }} as duels_won_pct,
    -- discipline
    sum(b.fouls_committed)                  as fouls_committed,
    sum(b.was_fouled)                       as was_fouled,
    -- rating
    round(avg(b.fotmob_rating)::numeric, 2) as avg_player_rating
from base b
left join matches m  on b.match_id       = m.match_id
left join teams   ht on m.home_team_id   = ht.team_id
left join teams   ot on m.away_team_id   = ot.team_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
