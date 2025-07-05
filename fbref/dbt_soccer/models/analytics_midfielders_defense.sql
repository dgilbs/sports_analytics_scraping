{{
  config(
    materialized='view'
  )
}}

select 
player, 
minutes,
{{ safe_divide_round('tackles_won', 'tackles_att') }} + tackles_won as tackling_score, 
interceptions, 
{{ safe_divide_round('challenges_won', 'challenges_att') }} + challenges_won as challenge_score,
(tackles_def_third * 1) + (tackles_mid_third * 2) + (tackles_att_third * 1) as midfield_tackling_score,
blocks
from {{ ref('player_reporting_defense') }}
where is_midfielder





-- 1. Tackle Success Rate - Efficiency in winning possession
-- Formula: (tackles_won / tackles_att) × 100
-- 2. Pressing Effectiveness - High-press defensive contribution
-- Formula: tackles_att_third + challenges_att_third
-- 3. Interception Rate per Minute - Reading the game ability
-- Formula: interceptions / minutes
-- 4.Midfield Disruption - Breaking up opposition play
-- Formula: tackles_mid_third + interceptions
-- 5.Defensive Transition - Winning ball in different zones
-- Formula: (tackles_def_third + tackles_mid_third + tackles_att_third) / tackles_won × 100
-- 6.Challenge Dominance - Physical presence in midfield
-- Formula: (challenges_won / challenges_att) × 100
-- 7.Clean Defensive Impact - Positive defensive actions without errors
-- 8. Formula: (tackles_won + interceptions + blocks) - errors_lead_to_shot