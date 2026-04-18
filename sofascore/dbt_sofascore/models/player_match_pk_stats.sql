select
    event_id,
    player_id,
    count(*)                        as pk_attempts,
    sum(is_goal::int)               as pk_goals,
    sum((not is_goal)::int)         as pk_missed
from {{ source('sofascore', 'fact_shots') }}
where situation = 'penalty'
group by event_id, player_id
