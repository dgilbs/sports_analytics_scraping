-- Converts dribble/carry map zone percentages into estimated counts.
-- All counts are rounded integers derived from: pct * total dribbles/carries.

with base as (
    select * from {{ source('sofascore', 'fact_drib_maps') }}
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

    -- Totals (already counts)
    dribbles_won,
    dribbles_lost,
    dribbles_total,
    dribble_success,
    carry_segments,

    -- Dribble zone counts (all dribbles)
    coalesce(round(dribbles_total * drib_def_third),  0)  as drib_def_third_count,
    coalesce(round(dribbles_total * drib_mid_third),  0)  as drib_mid_third_count,
    coalesce(round(dribbles_total * drib_att_third),  0)  as drib_att_third_count,
    coalesce(round(dribbles_total * drib_left_wing),  0)  as drib_left_wing_count,
    coalesce(round(dribbles_total * drib_central),    0)  as drib_central_count,
    coalesce(round(dribbles_total * drib_right_wing), 0)  as drib_right_wing_count,

    -- Dribble won zone counts
    coalesce(round(dribbles_won * drib_won_def_third), 0)  as drib_won_def_third_count,
    coalesce(round(dribbles_won * drib_won_mid_third), 0)  as drib_won_mid_third_count,
    coalesce(round(dribbles_won * drib_won_att_third), 0)  as drib_won_att_third_count,

    -- Carry zone counts
    coalesce(round(carry_segments * carry_def_third),  0)  as carry_def_third_count,
    coalesce(round(carry_segments * carry_mid_third),  0)  as carry_mid_third_count,
    coalesce(round(carry_segments * carry_att_third),  0)  as carry_att_third_count,
    coalesce(round(carry_segments * carry_left_wing),  0)  as carry_left_wing_count,
    coalesce(round(carry_segments * carry_central),    0)  as carry_central_count,
    coalesce(round(carry_segments * carry_right_wing), 0)  as carry_right_wing_count

from base
