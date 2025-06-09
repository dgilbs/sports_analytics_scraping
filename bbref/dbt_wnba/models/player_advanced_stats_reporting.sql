{{
  config(
    materialized='view',
    description='WNBA player performance metrics filtered by date range'
  )
}}

{% set start_date = var('start_date', '2024-05-15') %}
{% set end_date = var('end_date', '2024-09-20') %}

select 
*
from {{ ref('player_game_advanced_box_scores') }}
where game_date between '{{ start_date }}' and '{{ end_date }}'
