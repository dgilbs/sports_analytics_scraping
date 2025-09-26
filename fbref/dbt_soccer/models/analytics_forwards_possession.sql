{{
  config(
    materialized='view'
  )
}}


select
poss.player,
poss.minutes,
round(poss.touches_att_penalty_area::numeric/nullif(poss.touches, 0), 4) as penalty_area_touch_pct,
progressive_passes_received,
progressive_carries,
carries_into_penalty_area,
round(poss.touches_att_third::numeric/nullif(poss.touches, 0), 4) as attacking_third_touch_pct,
take_ons_succeeded,
round(successful_carries::numeric/nullif(carries, 0), 4) as ball_retention_index
from 
{{ ref('player_reporting_possession')}} poss 
where is_forward = true






-- 1. Penalty area touch % - Goal threat positioning
-- 2. Progressive passes received - Link-up play involvement
-- 3. Dribble success rate - Ability to beat defenders
-- 4. Carries into penalty area - Creating own chances
-- 5. Attacking third touch % - Overall final third presence
-- 6. Progressive carries - Ability to advance play
-- 7. Ball retention rate - Security in tight spaces