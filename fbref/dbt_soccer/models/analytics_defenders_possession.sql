{{
  config(
    materialized='view'
  )
}}

with cte as (
select
poss.player,
poss.minutes,
round(poss.touches/poss.minutes::numeric, 4) as touches_per_minute,
poss.progressive_carries,
pass.passes_completed, 
pass.passes_attempted,
poss.successful_carries,
poss.lost_carries,
poss.total_carries_distance,
poss.take_ons_succeeded,
poss.carries_into_final_third,
round(poss.touches_att_third/poss.touches::numeric, 4) as attacking_involvement,
successful_carries + passes_completed + take_ons_succeeded as successful_actions,
passes_attempted + carries + take_ons_attempted as total_actions
from 
{{ ref('player_reporting_possession')}} poss 
left join {{ ref('player_reporting_passing')}} pass 
on pass.appearance_id = poss.appearance_id
where poss.is_defender = true
)
select 
player, 
minutes,
progressive_carries, 
touches_per_minute,
attacking_involvement,
total_carries_distance,
take_ons_succeeded,
carries_into_final_third,
round(successful_actions::numeric/total_actions * 100, 4) as ball_retention_index
from cte
