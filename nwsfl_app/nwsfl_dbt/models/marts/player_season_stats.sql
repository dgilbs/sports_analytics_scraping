{{
    config(
        materialized='table',
        tags=['daily']
    )
}}

-- Mart: Player season statistics
-- Aggregated player performance by season
select
    player_id
    , player_name
    , count(distinct match_id) as matches_played
    , sum(minutes_played) as total_minutes
    , sum(goals) as total_goals
    , sum(assists) as total_assists
    , sum(shots_total) as total_shots
    , sum(shots_on_target) as shots_on_target
    , round(
        case 
            when sum(shots_total) > 0 
            then sum(goals)::numeric / sum(shots_total)::numeric * 100
            else 0 
        end, 2
    ) as shooting_accuracy_pct
    , sum(yellow_cards) as total_yellow_cards
    , sum(red_cards) as total_red_cards
    , sum(tackles) as total_tackles
    , round(sum(minutes_played)::numeric / count(distinct match_id)::numeric, 1) as avg_minutes_per_match
    , round(sum(goals)::numeric / count(distinct match_id)::numeric, 2) as goals_per_match
    , round(sum(assists)::numeric / count(distinct match_id)::numeric, 2) as assists_per_match
from {{ ref('int_player_performance') }}
group by 
    player_id
    , player_name
having count(distinct match_id) >= 5  -- Only players with 5+ matches
order by total_goals desc, total_assists desc

