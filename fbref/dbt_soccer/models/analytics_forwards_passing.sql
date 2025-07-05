{{
  config(
    materialized='view'
  )
}}

select 
player,
pass.minutes,
{{safe_divide_round('passes_completed', 'passes_attempted')}} as pass_completion,
key_passes,
round(assists::numeric + xa::numeric, 4) as chance_creation,
passes_into_penalty_area,
crosses_into_penalty_area,
xag,
passes_completed + (key_passes * 2) + (assists * 3) as link_up_score
from {{ ref('player_reporting_passing') }} pass
where is_forward = true


-- Pass Completion Rate - Link-up play reliability
-- Key Passes - Chance creation ability for teammates
-- Assists + xA - Final product delivery and assist quality
-- Passes into Penalty Area - Box service and final ball quality
-- Crosses into Penalty Area - Wide service effectiveness
-- xAG (Expected Assisted Goals) - Quality of chances created
-- Link-up Score - Overall passing contribution (weighted formula)
--passes_completed + (key_passes × 2) + (assists × 3)