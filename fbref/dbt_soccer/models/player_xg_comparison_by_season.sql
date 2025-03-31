{{
    config(
        materialized="view",
    )
}}



select 
player,
season,
competition,
sum(goals) goals,
sum(xg) xg,
sum(goals) - sum(xg) as xg_diff
from soccer.player_match_summary
group by 1,2,3