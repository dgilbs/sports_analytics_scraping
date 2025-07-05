{{
  config(
    materialized='view'
  )
}}

select 
player,
minutes,
{{ safe_divide_round('tackles_won', 'tackles_att') }} + tackles_won as tackling_score, 
{{ safe_divide_round('challenges_won', 'challenges_att') }} + challenges_won as challenge_score,
tackles_mid_third + tackles_def_third as tackles_outside_attacking_third,
challenges_att + tackles_att as defensive_pressure,
interceptions,
blocks
from {{ ref('player_reporting_defense') }}
where is_forward





-- 1. High Press Tackles - Winning ball in attacking third
-- Direct metric: tackles_att_third
-- 2. Tackle Success Rate - Making defensive attempts count
-- Formula: (tackles_won / tackles_att) × 100
-- 3. Defensive Work Rate - Overall defensive contribution per minute
-- Formula: tackles_att / minutes
-- 4.Total Defensive Pressure - Combined defensive attempts
-- Formula: tackles_att + challenges_att
-- 5.Transition Defense - Helping win ball back quickly
-- Formula: interceptions + tackles_won
-- 6.Forward Block Impact - Disrupting from advanced positions
-- Formula: blocks + pass_blocks
-- 7.Error-Free Contribution - Defensive actions without mistakes
-- Formula: (tackles_won + interceptions + blocks) - errors_lead_to_shot