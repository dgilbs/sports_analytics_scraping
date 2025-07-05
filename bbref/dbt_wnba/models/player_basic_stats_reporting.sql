{{
  config(
    materialized='view',
    description='WNBA player performance metrics filtered by date range'
  )
}}

{% set start_date = var('start_date', '2024-05-15') %}
{% set end_date = var('end_date', '2024-09-20') %}

select 
player,
team,
player_id,
playing_position,
is_center,
is_guard,
is_forward,
is_post,
is_perimeter,
count(distinct game_id) games_played,
sum(field_goals) as field_goals,
sum(field_goal_attempts) as field_goal_attempts,
sum(field_goals_threes) as field_goals_threes,
sum(field_goal_threes_attempts) as field_goal_threes_attempts,
sum(free_throws) as free_throws,
sum(free_throw_attempts) AS free_throw_attempts,
sum(offensive_rebounds) AS offensive_rebounds,
sum(defensive_rebounds) as defensive_rebounds,
sum(total_rebounds) AS total_rebounds,
sum(assists) AS assists, 
sum(steals) AS steals,
sum(blocks) AS blocks,
sum(turnovers) AS turnovers,
sum(personal_fouls) AS personal_fouls, 
sum(plus_minus) AS plus_minus,
sum(points) as points,
round(sum(minutes_played)::numeric, 4) AS minutes_played
from {{ ref('player_game_box_scores') }}
where game_date between '{{ start_date }}' and '{{ end_date }}'
group by 1,2,3,4,5,6,7,8,9

