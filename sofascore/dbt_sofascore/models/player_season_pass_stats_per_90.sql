with base as (
    select * from {{ ref('player_season_pass_stats') }}
)

select
    *,

    {{ per90('passes_total',        'total_minutes') }} as passes_per90,
    {{ per90('passes_accurate',     'total_minutes') }} as accurate_passes_per90,
    {{ per90('progressive_passes',  'total_minutes') }} as progressive_passes_per90,

    -- Direction
    {{ per90('passes_forward',      'total_minutes') }} as passes_forward_per90,
    {{ per90('passes_backward',     'total_minutes') }} as passes_backward_per90,
    {{ per90('passes_lateral',      'total_minutes') }} as passes_lateral_per90,

    -- Origin zones
    {{ per90('origin_def_third',    'total_minutes') }} as origin_def_third_per90,
    {{ per90('origin_mid_third',    'total_minutes') }} as origin_mid_third_per90,
    {{ per90('origin_att_third',    'total_minutes') }} as origin_att_third_per90,

    -- Destination zones
    {{ per90('dest_def_third',      'total_minutes') }} as dest_def_third_per90,
    {{ per90('dest_mid_third',      'total_minutes') }} as dest_mid_third_per90,
    {{ per90('dest_att_third',      'total_minutes') }} as dest_att_third_per90

from base
