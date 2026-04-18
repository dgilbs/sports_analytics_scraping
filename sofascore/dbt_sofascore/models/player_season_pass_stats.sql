with passes as (
    select * from {{ ref('pass_map_counts') }}
),

mins as (
    select event_id, player_id, minutes_played
    from {{ ref('player_match_stats') }}
    where minutes_played > 0
),

joined as (
    select
        p.*,
        m.minutes_played
    from passes p
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

    -- Totals
    sum(passes_total)                                                                           as passes_total,
    sum(passes_accurate)                                                                        as passes_accurate,
    sum(passes_inaccurate)                                                                      as passes_inaccurate,
    sum(progressive_passes)                                                                     as progressive_passes,
    {{ safe_divide_round('sum(passes_accurate)', 'sum(passes_total)') }}                        as pass_accuracy,
    {{ safe_divide_round('sum(progressive_passes)', 'sum(passes_total)') }}                     as progressive_pass_pct,
    {{ safe_divide_round('sum(avg_pass_length * passes_total)', 'sum(passes_total)', 1) }}      as avg_pass_length,

    -- Direction counts (all passes)
    sum(passes_forward)                                                                         as passes_forward,
    sum(passes_backward)                                                                        as passes_backward,
    sum(passes_lateral)                                                                         as passes_lateral,

    -- Direction counts (accurate passes)
    sum(acc_passes_forward)                                                                     as acc_passes_forward,
    sum(acc_passes_backward)                                                                    as acc_passes_backward,
    sum(acc_passes_lateral)                                                                     as acc_passes_lateral,

    -- Origin zones
    sum(origin_def_third_count)                                                                 as origin_def_third,
    sum(origin_mid_third_count)                                                                 as origin_mid_third,
    sum(origin_att_third_count)                                                                 as origin_att_third,
    sum(origin_left_wing_count)                                                                 as origin_left_wing,
    sum(origin_central_count)                                                                   as origin_central,
    sum(origin_right_wing_count)                                                                as origin_right_wing,

    -- Destination zones
    sum(dest_def_third_count)                                                                   as dest_def_third,
    sum(dest_mid_third_count)                                                                   as dest_mid_third,
    sum(dest_att_third_count)                                                                   as dest_att_third,
    sum(dest_left_wing_count)                                                                   as dest_left_wing,
    sum(dest_central_count)                                                                     as dest_central,
    sum(dest_right_wing_count)                                                                  as dest_right_wing,

    -- Accurate destination zones
    sum(acc_dest_def_third_count)                                                               as acc_dest_def_third,
    sum(acc_dest_mid_third_count)                                                               as acc_dest_mid_third,
    sum(acc_dest_att_third_count)                                                               as acc_dest_att_third,
    sum(acc_dest_left_wing_count)                                                               as acc_dest_left_wing,
    sum(acc_dest_central_count)                                                                 as acc_dest_central,
    sum(acc_dest_right_wing_count)                                                              as acc_dest_right_wing,

    -- Penalty area & final third
    sum(passes_into_final_third)                                                                as passes_into_final_third,
    sum(acc_passes_into_final_third)                                                            as acc_passes_into_final_third,
    sum(passes_into_penalty_area)                                                               as passes_into_penalty_area,
    sum(acc_passes_into_penalty_area)                                                           as acc_passes_into_penalty_area,
    sum(crosses_into_penalty_area)                                                              as crosses_into_penalty_area,

    -- Pass length buckets
    sum(passes_short)                                                                           as passes_short,
    sum(passes_medium)                                                                          as passes_medium,
    sum(passes_long)                                                                            as passes_long,
    sum(acc_passes_short)                                                                       as acc_passes_short,
    sum(acc_passes_medium)                                                                      as acc_passes_medium,
    sum(acc_passes_long)                                                                        as acc_passes_long

from joined
group by player_id, player_name, season, team, position
