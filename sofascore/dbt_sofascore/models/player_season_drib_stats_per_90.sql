with base as (
    select * from {{ ref('player_season_drib_stats') }}
)

select
    *,

    {{ per90('dribbles_total',      'total_minutes') }} as dribbles_per90,
    {{ per90('dribbles_won',        'total_minutes') }} as dribbles_won_per90,
    {{ per90('carry_segments',      'total_minutes') }} as carries_per90,

    -- Dribble zones
    {{ per90('drib_def_third',      'total_minutes') }} as drib_def_third_per90,
    {{ per90('drib_mid_third',      'total_minutes') }} as drib_mid_third_per90,
    {{ per90('drib_att_third',      'total_minutes') }} as drib_att_third_per90,

    -- Carry zones
    {{ per90('carry_def_third',     'total_minutes') }} as carry_def_third_per90,
    {{ per90('carry_mid_third',     'total_minutes') }} as carry_mid_third_per90,
    {{ per90('carry_att_third',     'total_minutes') }} as carry_att_third_per90

from base
