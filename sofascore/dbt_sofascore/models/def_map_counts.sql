-- Converts defensive map zone percentages into estimated counts.
-- All counts are rounded integers derived from: pct * relevant action total.

with base as (
    select * from {{ source('sofascore', 'fact_def_maps') }}
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
    tackle_won,
    missed_tackle,
    interception,
    clearance,
    block,
    recovery,
    total_def_actions,
    tackle_success,

    -- All defensive actions by zone
    coalesce(round(total_def_actions * pct_def_third),  0)  as def_actions_def_third_count,
    coalesce(round(total_def_actions * pct_mid_third),  0)  as def_actions_mid_third_count,
    coalesce(round(total_def_actions * pct_att_third),  0)  as def_actions_att_third_count,
    coalesce(round(total_def_actions * pct_left_wing),  0)  as def_actions_left_wing_count,
    coalesce(round(total_def_actions * pct_central),    0)  as def_actions_central_count,
    coalesce(round(total_def_actions * pct_right_wing), 0)  as def_actions_right_wing_count,

    -- Tackles won by zone
    coalesce(round(tackle_won * tackle_def_third), 0)  as tackle_won_def_third_count,
    coalesce(round(tackle_won * tackle_mid_third), 0)  as tackle_won_mid_third_count,
    coalesce(round(tackle_won * tackle_att_third), 0)  as tackle_won_att_third_count,

    -- Interceptions by zone
    coalesce(round(interception * intercept_def_third), 0)  as intercept_def_third_count,
    coalesce(round(interception * intercept_mid_third), 0)  as intercept_mid_third_count,
    coalesce(round(interception * intercept_att_third), 0)  as intercept_att_third_count,

    -- Recoveries by zone
    coalesce(round(recovery * recovery_def_third), 0)  as recovery_def_third_count,
    coalesce(round(recovery * recovery_mid_third), 0)  as recovery_mid_third_count,
    coalesce(round(recovery * recovery_att_third), 0)  as recovery_att_third_count

from base
