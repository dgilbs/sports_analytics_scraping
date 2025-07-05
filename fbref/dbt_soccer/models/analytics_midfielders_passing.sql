{{
  config(
    materialized='view'
  )
}}

select 
player,
minutes,
{{ safe_divide_round('passes_completed', 'passes_attempted') }} as pass_completion_pct,
{{ safe_divide_round('progressive_passes', 'passes_attempted') }} as progressive_pass_rate,
key_passes + passes_into_penalty_area as final_third_service,
xa,
{{ safe_divide_round('passes_attempted', 'minutes') }} as passes_per_minute,
assists + passes_into_final_third as attacking_output,
round(((key_passes * 3) + (assists * 5) + (passes_into_penalty_area * 2))/passes_attempted::numeric, 4) as weighted_creative_output
from {{ ref('player_reporting_passing')}}
where is_midfielder = true and passes_attempted > 0 



-- Pass Completion Rate - Overall accuracy and reliability
-- Formula: (passes_completed / passes_attempted) × 100
-- Progressive Pass Rate - Forward-thinking play percentage
-- Formula: (progressive_passes / passes_attempted) × 100
-- Through Ball Creation - Key passes + passes into penalty area
-- Formula: key_passes + passes_into_penalty_area
-- Final Third Service - Ability to deliver balls into dangerous areas
-- Formula: passes_into_final_third + crosses_into_penalty_area
-- Expected Assist Value (xA) - Quality of chance creation
-- Direct metric: xa
-- Pass Volume per Minute - Central involvement in possession
-- Formula: passes_attempted / minutes
-- Creative Passing Efficiency - Weighted creative output per attempt
-- Formula: ((key_passes × 3) + (assists × 5) + (passes_into_penalty_area × 2)) / passes_attempted × 100