{{
    config(
        materialized='view',
        tags=['daily']
    )
}}

-- Staging model for match data
-- Standardizes match information
select
    id as match_key
    , match_date
    , home_team_id
    , away_team_id
    , home_goals::int as home_goals
    , away_goals::int as away_goals
    , home_xg::float as home_expected_goals
    , away_xg::float as away_expected_goals
    , competition_id
    , season
    , venue
    , referee
    , attendance::int as attendance
from {{ source('nwsfl', 'dim_matches') }}
where match_date is not null

