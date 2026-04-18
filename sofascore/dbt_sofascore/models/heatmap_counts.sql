-- Converts heatmap zone fractions into estimated touch counts.
-- All counts are rounded integers derived from: touch_count * zone_fraction.

with base as (
    select * from {{ source('sofascore', 'fact_heatmaps') }}
)

select
    event_id,
    player_id,
    season,
    match_date,
    home_team,
    away_team,
    team,
    side,
    player_name,
    position,
    substitute,

    touch_count,

    coalesce(round(touch_count * defensive_third), 0)  as def_third_touches,
    coalesce(round(touch_count * middle_third),    0)  as mid_third_touches,
    coalesce(round(touch_count * attacking_third), 0)  as att_third_touches,
    coalesce(round(touch_count * left_wing),         0)  as left_wing_touches,
    coalesce(round(touch_count * central),           0)  as central_touches,
    coalesce(round(touch_count * right_wing),        0)  as right_wing_touches,
    coalesce(round(touch_count * att_penalty_area),  0)  as att_penalty_area_touches

from base
