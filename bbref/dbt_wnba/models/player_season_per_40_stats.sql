
{{
    config(
        materialized="view",
    )
}}


with t as (
select 
player,
season,
count(*) as num_games,
sum(minutes_played) as minutes_played,
sum(points) as points,
sum(assists) as assists,
sum(total_rebounds) as total_rebounds
from {{ ref ('player_game_box_scores')}}
group by 1,2
)
SELECT
player,
season,
minutes_played::numeric/num_games as minutes_per_game,
{{ per_40_stats('points', 'minutes_played') }} AS points,
{{ per_40_stats('assists', 'minutes_played') }} AS assists,
{{ per_40_stats('total_rebounds', 'minutes_played') }} AS total_rebounds
from t

