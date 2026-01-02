{{
    config(
        materialized='ephemeral',
        tags=['daily']
    )
}}

-- Intermediate model: Player performance aggregations
-- Combines player info with their match statistics
select
    p.player_key
    , p.player_name
    , s.player_id
    , s.team_id
    , s.match_id
    , s.minutes_played
    , s.goals
    , s.assists
    , s.shots_total
    , s.shots_on_target
    , s.touches
    , s.yellow_cards
    , s.red_cards
    , s.tackles
from {{ ref('stg_players') }} p
inner join {{ ref('stg_player_match_stats') }} s
    on p.player_key = s.player_id

