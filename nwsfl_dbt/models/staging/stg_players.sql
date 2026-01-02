{{
    config(
        materialized='view',
        tags=['daily']
    )
}}

-- Staging model for player data
-- Cleans and standardizes player information from source
select
    id as player_key
    , player as player_name
from {{ source('nwsfl', 'dim_players') }}

