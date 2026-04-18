with base as (
    select * from {{ ref('player_season_heatmap_stats') }}
)

select
    *,

    {{ per90('total_touches',       'total_minutes') }} as touches_per90,
    {{ per90('def_third_touches',   'total_minutes') }} as def_third_touches_per90,
    {{ per90('mid_third_touches',   'total_minutes') }} as mid_third_touches_per90,
    {{ per90('att_third_touches',   'total_minutes') }} as att_third_touches_per90,
    {{ per90('left_wing_touches',   'total_minutes') }} as left_wing_touches_per90,
    {{ per90('central_touches',     'total_minutes') }} as central_touches_per90,
    {{ per90('right_wing_touches',  'total_minutes') }} as right_wing_touches_per90

from base
