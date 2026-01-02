{{
    config(
        materialized='view',
        tags=['daily']
    )
}}

-- Staging model for player match performance
-- Core statistics for each player appearance
select
    id as appearance_key
    , player_id
    , match_id
    , team_id
    , minutes::int as minutes_played
    , goals::int as goals
    , assists::int as assists
    , pk_goals::int as penalties_scored
    , pk_attempts::int as penalties_attempted
    , shots::int as shots_total
    , shots_on_target::int as shots_on_target
    , yellow_cards::int as yellow_cards
    , red_cards::int as red_cards
    , touches::int as touches
    , tackles::int as tackles
from {{ source('nwsfl', 'f_player_match_summary') }}
where minutes is not null
    and minutes::int > 0

