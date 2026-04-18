select
    player_id,
    player_name,
    season,
    count(*)                        as pk_attempts,
    sum(is_goal::int)               as pk_goals,
    sum((not is_goal)::int)         as pk_missed
from {{ source('sofascore', 'fact_shots') }}
where situation = 'penalty'
group by player_id, player_name, season
