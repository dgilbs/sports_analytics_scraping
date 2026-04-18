with base as (
    select * from {{ ref('player_season_def_stats') }}
)

select
    *,

    {{ per90('total_def_actions',       'total_minutes') }} as def_actions_per90,
    {{ per90('tackle_won',              'total_minutes') }} as tackles_won_per90,
    {{ per90('missed_tackle',           'total_minutes') }} as missed_tackles_per90,
    {{ per90('interception',            'total_minutes') }} as interceptions_per90,
    {{ per90('clearance',               'total_minutes') }} as clearances_per90,
    {{ per90('block',                   'total_minutes') }} as blocks_per90,
    {{ per90('recovery',                'total_minutes') }} as recoveries_per90,

    -- By zone
    {{ per90('def_actions_def_third',   'total_minutes') }} as def_actions_def_third_per90,
    {{ per90('def_actions_mid_third',   'total_minutes') }} as def_actions_mid_third_per90,
    {{ per90('def_actions_att_third',   'total_minutes') }} as def_actions_att_third_per90

from base
