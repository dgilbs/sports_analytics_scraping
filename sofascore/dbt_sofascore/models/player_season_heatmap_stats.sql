with heatmap as (
    select * from {{ ref('heatmap_counts') }}
),

mins as (
    select event_id, player_id, minutes_played
    from {{ ref('player_match_stats') }}
    where minutes_played > 0
),

joined as (
    select
        h.*,
        m.minutes_played
    from heatmap h
    inner join mins m using (event_id, player_id)
)

select
    player_id,
    player_name,
    season,
    team,
    position,

    count(*)                as matches_played,
    sum(minutes_played)     as total_minutes,

    sum(touch_count)            as total_touches,
    sum(def_third_touches)      as def_third_touches,
    sum(mid_third_touches)      as mid_third_touches,
    sum(att_third_touches)      as att_third_touches,
    sum(left_wing_touches)      as left_wing_touches,
    sum(central_touches)        as central_touches,
    sum(right_wing_touches)     as right_wing_touches,

    -- Zone fractions recomputed from season totals
    {{ safe_divide_round('sum(def_third_touches)', 'sum(touch_count)') }}   as pct_def_third,
    {{ safe_divide_round('sum(mid_third_touches)', 'sum(touch_count)') }}   as pct_mid_third,
    {{ safe_divide_round('sum(att_third_touches)', 'sum(touch_count)') }}   as pct_att_third,
    {{ safe_divide_round('sum(left_wing_touches)', 'sum(touch_count)') }}         as pct_left_wing,
    {{ safe_divide_round('sum(central_touches)',   'sum(touch_count)') }}         as pct_central,
    {{ safe_divide_round('sum(right_wing_touches)','sum(touch_count)') }}         as pct_right_wing,

    sum(att_penalty_area_touches)                                                 as att_penalty_area_touches,
    {{ safe_divide_round('sum(att_penalty_area_touches)', 'sum(touch_count)') }}  as pct_att_penalty_area

from joined
group by player_id, player_name, season, team, position
