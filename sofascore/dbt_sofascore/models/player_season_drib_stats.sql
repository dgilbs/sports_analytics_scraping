with drib as (
    select * from {{ ref('drib_map_counts') }}
),

mins as (
    select event_id, player_id, minutes_played
    from {{ ref('player_match_stats') }}
    where minutes_played > 0
),

joined as (
    select
        d.*,
        m.minutes_played
    from drib d
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

    -- Action totals
    sum(dribbles_won)                                                                           as dribbles_won,
    sum(dribbles_lost)                                                                          as dribbles_lost,
    sum(dribbles_total)                                                                         as dribbles_total,
    sum(carry_segments)                                                                         as carry_segments,
    {{ safe_divide_round('sum(dribbles_won)', 'sum(dribbles_total)') }}                         as dribble_success,

    -- Dribble zone counts (all dribbles)
    sum(drib_def_third_count)                                                                   as drib_def_third,
    sum(drib_mid_third_count)                                                                   as drib_mid_third,
    sum(drib_att_third_count)                                                                   as drib_att_third,
    sum(drib_left_wing_count)                                                                   as drib_left_wing,
    sum(drib_central_count)                                                                     as drib_central,
    sum(drib_right_wing_count)                                                                  as drib_right_wing,

    -- Dribbles won by zone
    sum(drib_won_def_third_count)                                                               as drib_won_def_third,
    sum(drib_won_mid_third_count)                                                               as drib_won_mid_third,
    sum(drib_won_att_third_count)                                                               as drib_won_att_third,

    -- Carry zone counts
    sum(carry_def_third_count)                                                                  as carry_def_third,
    sum(carry_mid_third_count)                                                                  as carry_mid_third,
    sum(carry_att_third_count)                                                                  as carry_att_third,
    sum(carry_left_wing_count)                                                                  as carry_left_wing,
    sum(carry_central_count)                                                                    as carry_central,
    sum(carry_right_wing_count)                                                                 as carry_right_wing,

    -- Penalty area & final third
    sum(carries_into_final_third)                                                               as carries_into_final_third,
    sum(carries_into_penalty_area)                                                              as carries_into_penalty_area

from joined
group by player_id, player_name, season, team, position
