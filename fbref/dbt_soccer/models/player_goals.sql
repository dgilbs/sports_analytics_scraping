{{
    config(
        materialized="view",
    )
}}



select 
player,
season,
sum(goals) goals
from soccer.player_match_summary
group by 1,2