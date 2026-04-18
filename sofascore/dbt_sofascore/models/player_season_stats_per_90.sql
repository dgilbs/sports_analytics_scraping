with base as (
    select * from {{ ref('player_season_stats') }}
)

select
    *,

    -- Goals & Attacking
    {{ per90('shots_on_target',     'total_minutes') }} as shots_on_target_per90,
    {{ per90('big_chances_missed',  'total_minutes') }} as big_chances_missed_per90,

    -- Passing
    {{ per90('total_passes',        'total_minutes') }} as passes_per90,
    {{ per90('accurate_passes',     'total_minutes') }} as accurate_passes_per90,
    {{ per90('total_long_balls',    'total_minutes') }} as long_balls_per90,
    {{ per90('total_crosses',       'total_minutes') }} as crosses_per90,
    {{ per90('progressive_carries', 'total_minutes') }} as progressive_carries_per90,

    -- Touches & Possession
    {{ per90('touches',             'total_minutes') }} as touches_per90,
    {{ per90('possession_lost',     'total_minutes') }} as possession_lost_per90,

    -- Duels
    {{ per90('duels_won',           'total_minutes') }} as duels_won_per90,
    {{ per90('duels_lost',          'total_minutes') }} as duels_lost_per90,
    {{ per90('aerial_won',          'total_minutes') }} as aerial_won_per90,
    {{ per90('aerial_lost',         'total_minutes') }} as aerial_lost_per90,

    -- Defending
    {{ per90('tackles_won',         'total_minutes') }} as tackles_won_per90,
    {{ per90('clearances',          'total_minutes') }} as clearances_per90,

    -- Discipline
    {{ per90('fouls',               'total_minutes') }} as fouls_per90,
    {{ per90('was_fouled',          'total_minutes') }} as was_fouled_per90

from base
