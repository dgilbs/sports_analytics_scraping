{{
  config(
    materialized='view'
  )
}}


select 
player,
minutes,
{{ safe_divide_round('tackles_won', 'tackles_att') }} + tackles_won as tackling_score,
tackles_won + interceptions + clearances + blocks as defensive_actions,
(tackles_def_third * 1) + (tackles_mid_third * 2) + (tackles_att_third * 3) as high_press_score,
clearances,
blocks + interceptions as break_up_play_score,
{{ safe_divide_round('challenges_won', 'challenges_att') }} + challenges_won as challenge_score
from {{ ref('player_reporting_defense') }}
where is_defender

-- 1. Tackle Dominance - Success rate with volume reward
-- Formula: tackles_won_pct + tackles_won
-- 2. Defensive Actions per Minute - Overall defensive workload
-- Formula: (tackles_won + interceptions + clearances + blocks) / minutes
-- 3. Zone Spread Score - Tackling versatility across areas
-- Formula: tackles_def_third + tackles_mid_third + tackles_att_third
-- 4. Clearance Dominance - Ability to clear danger
-- Direct metric: clearances
-- 5. Shot Prevention Impact - Blocking dangerous attempts
-- Formula: shot_blocks + pass_blocks
-- 6. Challenge Dominance - Physical duel success with volume reward
-- Formula: challenges_won_pct + challenges_won
-- 7.Error-Free Defending - Reliability under pressure
-- Formula: (tackles_won + interceptions + clearances + blocks) - errors_lead_to_shot