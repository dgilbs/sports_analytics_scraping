{{
  config(
    materialized='view'
  )
}}

{% set start_date = var('start_date', '2024-01-01') %}
{% set end_date = var('end_date', '2025-12-31') %}

select *  
from {{ ref('player_match_passing_types') }}
where match_date between '{{ start_date }}' and '{{ end_date }}' and competition = 'NWSL'
