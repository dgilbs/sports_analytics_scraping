-- Converts pass map zone/direction percentages into estimated counts.
-- All counts are rounded integers derived from: pct * total passes.

with base as (
    select * from {{ source('sofascore', 'fact_pass_maps') }}
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
    passes_total,
    passes_accurate,
    passes_inaccurate,
    pass_accuracy,
    avg_pass_length,
    progressive_passes,
    progressive_pass_pct,

    -- Direction counts (all passes)
    coalesce(round(passes_total * pct_forward),  0)  as passes_forward,
    coalesce(round(passes_total * pct_backward), 0)  as passes_backward,
    coalesce(round(passes_total * pct_lateral),  0)  as passes_lateral,

    -- Direction counts (accurate passes only)
    coalesce(round(passes_accurate * acc_pct_forward),  0)  as acc_passes_forward,
    coalesce(round(passes_accurate * acc_pct_backward), 0)  as acc_passes_backward,
    coalesce(round(passes_accurate * acc_pct_lateral),  0)  as acc_passes_lateral,

    -- Origin zone counts (where the pass started)
    coalesce(round(passes_total * origin_def_third),  0)  as origin_def_third_count,
    coalesce(round(passes_total * origin_mid_third),  0)  as origin_mid_third_count,
    coalesce(round(passes_total * origin_att_third),  0)  as origin_att_third_count,
    coalesce(round(passes_total * origin_left_wing),  0)  as origin_left_wing_count,
    coalesce(round(passes_total * origin_central),    0)  as origin_central_count,
    coalesce(round(passes_total * origin_right_wing), 0)  as origin_right_wing_count,

    -- Destination zone counts (where the pass ended)
    coalesce(round(passes_total * dest_def_third),  0)  as dest_def_third_count,
    coalesce(round(passes_total * dest_mid_third),  0)  as dest_mid_third_count,
    coalesce(round(passes_total * dest_att_third),  0)  as dest_att_third_count,
    coalesce(round(passes_total * dest_left_wing),  0)  as dest_left_wing_count,
    coalesce(round(passes_total * dest_central),    0)  as dest_central_count,
    coalesce(round(passes_total * dest_right_wing), 0)  as dest_right_wing_count,

    -- Accurate destination zone counts
    coalesce(round(passes_accurate * acc_dest_def_third),  0)  as acc_dest_def_third_count,
    coalesce(round(passes_accurate * acc_dest_mid_third),  0)  as acc_dest_mid_third_count,
    coalesce(round(passes_accurate * acc_dest_att_third),  0)  as acc_dest_att_third_count,
    coalesce(round(passes_accurate * acc_dest_left_wing),  0)  as acc_dest_left_wing_count,
    coalesce(round(passes_accurate * acc_dest_central),    0)  as acc_dest_central_count,
    coalesce(round(passes_accurate * acc_dest_right_wing), 0)  as acc_dest_right_wing_count,

    -- Penalty area & final third (already counts from scraper)
    coalesce(passes_into_final_third,         0)  as passes_into_final_third,
    coalesce(acc_passes_into_final_third,     0)  as acc_passes_into_final_third,
    coalesce(passes_into_penalty_area,        0)  as passes_into_penalty_area,
    coalesce(acc_passes_into_penalty_area,    0)  as acc_passes_into_penalty_area,
    coalesce(crosses_into_penalty_area,       0)  as crosses_into_penalty_area,

    -- Pass length buckets (Euclidean distance: short <15m, medium 15-32m, long >32m)
    coalesce(passes_short,      0)  as passes_short,
    coalesce(passes_medium,     0)  as passes_medium,
    coalesce(passes_long,       0)  as passes_long,
    coalesce(acc_passes_short,  0)  as acc_passes_short,
    coalesce(acc_passes_medium, 0)  as acc_passes_medium,
    coalesce(acc_passes_long,   0)  as acc_passes_long

from base
