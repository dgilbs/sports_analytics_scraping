{{
  config(
    materialized='view'
  )
}}


select 
player,
minutes,
round(touches::numeric/"minutes", 4) as touches_per_minute,
progressive_carries + progressive_passes_received as link_up_plays,
round(touches_att_third::numeric/nullif(touches, 0), 4) as attacking_involvement,
round(touches_def_third::numeric/nullif(touches, 0), 4) as defensive_involvement,
take_ons_succeeded,
carries_into_final_third,
round(successful_carries::numeric/nullif(carries, 0),4) as ball_retention_index
from {{ ref('player_reporting_possession')}} poss 
where is_midfielder = true


