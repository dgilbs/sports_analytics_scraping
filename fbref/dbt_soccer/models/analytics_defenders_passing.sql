{{
  config(
    materialized='view'
  )
}}


select 
player, 
minutes,
round(passes_completed::numeric/nullif(passes_attempted, 0), 4) as pass_completion_rate,
progressive_passes,
round(long_passes_completed::numeric/nullif(long_passes_attempted, 0), 4) as long_pass_accuracy,
passes_into_final_third,
key_passes + assists as creative_output,
round(passes_completed/"minutes", 4) as passes_per_minute,
(passes_completed * 0.5) + (progressive_passes * 2) + (key_passes * 3 )+ (assists * 5) as pass_efficiency_index
from {{ref('player_reporting_passing')}}
where is_defender = true


-- Pass Completion Rate - Team reliability and ball retention
-- Progressive Passes - Forward play initiation ability
-- Long Pass Accuracy - Switching play and range of passing
-- Passes into Final Third - Attacking contribution
-- Key Passes + Assists - Creative output from deep
-- Pass Efficiency Score - Overall passing impact (weighted formula)
-- Passes per Minute - Involvement in team build-up play

--Pass Efficiency Score = (passes_completed × 0.5) + (progressive_passes × 2) + (key_passes × 3) + (assists × 5)