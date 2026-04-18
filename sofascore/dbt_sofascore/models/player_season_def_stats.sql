with def as (
    select * from {{ ref('def_map_counts') }}
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
    from def d
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
    sum(tackle_won)                                                                             as tackle_won,
    sum(missed_tackle)                                                                          as missed_tackle,
    sum(interception)                                                                           as interception,
    sum(clearance)                                                                              as clearance,
    sum(block)                                                                                  as block,
    sum(recovery)                                                                               as recovery,
    sum(total_def_actions)                                                                      as total_def_actions,
    {{ safe_divide_round('sum(tackle_won)', 'sum(tackle_won + missed_tackle)') }}               as tackle_success,

    -- All defensive actions by zone
    sum(def_actions_def_third_count)                                                            as def_actions_def_third,
    sum(def_actions_mid_third_count)                                                            as def_actions_mid_third,
    sum(def_actions_att_third_count)                                                            as def_actions_att_third,
    sum(def_actions_left_wing_count)                                                            as def_actions_left_wing,
    sum(def_actions_central_count)                                                              as def_actions_central,
    sum(def_actions_right_wing_count)                                                           as def_actions_right_wing,

    -- Tackles won by zone
    sum(tackle_won_def_third_count)                                                             as tackle_won_def_third,
    sum(tackle_won_mid_third_count)                                                             as tackle_won_mid_third,
    sum(tackle_won_att_third_count)                                                             as tackle_won_att_third,

    -- Interceptions by zone
    sum(intercept_def_third_count)                                                              as intercept_def_third,
    sum(intercept_mid_third_count)                                                              as intercept_mid_third,
    sum(intercept_att_third_count)                                                              as intercept_att_third,

    -- Recoveries by zone
    sum(recovery_def_third_count)                                                               as recovery_def_third,
    sum(recovery_mid_third_count)                                                               as recovery_mid_third,
    sum(recovery_att_third_count)                                                               as recovery_att_third

from joined
group by player_id, player_name, season, team, position
