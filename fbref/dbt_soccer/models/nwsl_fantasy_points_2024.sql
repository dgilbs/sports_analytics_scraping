{{
    config(
        materialized="view",
    )
}}


SELECT 
player,
match_date,
squad,
opponent,
playing_position,
goals * goal_points 
+ assists * assist_points
+ interceptions * interception_points
+ goal_creating_actions * gca_points 
+ coalesce(saves, 0) * save_points
+ goals_against * goals_conceded_points
+ blocks * block_points 
+ is_minutes_points * played_60_mins
+ is_appearance * appearance_points
+ is_shutout * clean_sheet_points
+ yellow_cards * yellow_card_points
+ red_cards  * red_card_points
+ is_touches_points * touches_points
AS fantasy_points
FROM soccer.f_nwsl_fantasy_points