-- Calculated analytics metrics per player per season.
-- Pulls from player_season_profile which aggregates all data sources.
--
-- Notes on proxies / limitations:
--   xg      : Fotmob season total; NULL where player has no confirmed crossref entry
--   np_goal_efficiency : uses xg as npxg proxy
--   creative_playmaking_index : excludes xag (not available)
--   shot_quality_creation : uses xa only as proxy for xag + xa
--   ball_security_progression / possession_security_index : not computed at season level
--                                                           (dispossessed/unsuccessful_touch
--                                                            not in season aggregation)
--   progressive_carrying_impact : not computed at season level
--                                 (carries_distance not in season aggregation)
--   progressive_carry_distance_pct : not computed at season level (same reason)

with base as (
    select * from {{ ref('player_season_profile') }}
)

select
    player_id,
    player_name,
    season,
    team,
    position,
    matches_played,
    total_minutes,

    -- -------------------------------------------------------------------------
    -- Attacking metrics
    -- -------------------------------------------------------------------------

    -- Complete Attacking Threat
    {{ safe_divide_round(
        'goals + assists + shots_on_target + key_passes + coalesce(xg, 0)',
        'nullif(touches, 0)', 3) }}                                             as complete_attacking_threat,

    -- Clinical Finishing
    {{ safe_divide_round(
        'goals + shots_on_target',
        'nullif(total_shots + coalesce(xg, 0), 0)', 3) }}                      as clinical_finishing,

    -- Non-Penalty Goal Efficiency  [proxy: xg used as npxg]
    {{ safe_divide_round(
        'np_goals + shots_on_target',
        'nullif(total_shots + coalesce(xg, 0), 0)', 3) }}                      as np_goal_efficiency,

    -- Final Third Conversion
    {{ safe_divide_round(
        'total_shots + key_passes + coalesce(crosses_into_penalty_area, 0) + coalesce(carries_into_penalty_area, 0)',
        'nullif(att_third_touches, 0)', 3) }}                                   as final_third_conversion,

    -- Penalty Area Dominance
    {{ safe_divide_round(
        'coalesce(att_penalty_area_touches, 0) + total_shots + coalesce(crosses_into_penalty_area, 0) + coalesce(passes_into_penalty_area, 0)',
        'nullif(att_third_touches, 0)', 3) }}                                   as penalty_area_dominance,

    -- -------------------------------------------------------------------------
    -- Creative & playmaking metrics
    -- -------------------------------------------------------------------------

    -- Creative Playmaking Index  [excludes xag]
    {{ safe_divide_round(
        'assists + key_passes + coalesce(passes_into_penalty_area, 0) + coalesce(crosses_into_penalty_area, 0)',
        'nullif(total_passes, 0)', 3) }}                                        as creative_playmaking_index,

    -- Shot Quality Creation  [proxy: xa only]
    {{ safe_divide_round(
        'coalesce(xa, 0)',
        'nullif(total_shots + coalesce(xg, 0), 0)', 3) }}                      as shot_quality_creation,

    -- -------------------------------------------------------------------------
    -- Defensive metrics
    -- -------------------------------------------------------------------------

    -- Tackling Score
    -- tackles_won_90 × tackle_success_rate × (1 + tackles_att / 10)
    round((
        (tackles_won / nullif(total_minutes / 90.0, 0))
        * (tackles_won::float / nullif(total_tackles, 0))
        * (1 + total_tackles / 10.0)
    )::numeric, 3)                                                              as tackling_score,

    -- Defensive Impact Score
    {{ safe_divide_round(
        'tackles_won + interceptions + coalesce(block, 0) + clearances + aerial_won',
        'nullif(total_tackles + coalesce(dribbles_total, 0) + aerial_won + aerial_lost, 0)', 3) }} as defensive_impact_score,

    -- Defensive Anticipation Index
    {{ safe_divide_round(
        'interceptions',
        'nullif(interceptions + total_tackles, 0)', 3) }}                      as defensive_anticipation_index,

    -- Aerial Dominance Score
    round((
        (aerial_won::float / nullif(aerial_won + aerial_lost, 0))
        * aerial_won
    )::numeric, 3)                                                              as aerial_dominance_score,

    -- -------------------------------------------------------------------------
    -- Possession & technical metrics
    -- -------------------------------------------------------------------------

    -- Ball Progression Mastery
    {{ safe_divide_round(
        'coalesce(progressive_passes, 0) + progressive_carries + coalesce(passes_into_final_third, 0) + coalesce(carries_into_final_third, 0)',
        'nullif(total_passes + carries, 0)', 3) }}                              as ball_progression_mastery,

    -- Progressive Action Rate
    {{ safe_divide_round(
        'coalesce(progressive_passes, 0) + progressive_carries + coalesce(carries_into_final_third, 0) + coalesce(passes_into_final_third, 0)',
        'nullif(touches, 0)', 3) }}                                             as progressive_action_rate,

    -- -------------------------------------------------------------------------
    -- Defensive splits
    -- -------------------------------------------------------------------------

    -- Defensive Actions per 90
    -- total_def_actions / (minutes / 90)
    {{ safe_divide_round(
        'tackles_won + interceptions + coalesce(block, 0) + clearances + recoveries',
        'nullif(total_minutes / 90.0, 0)', 3) }}                                as def_actions_per90,

    -- Recovery Rate
    -- recoveries / total_def_actions
    {{ safe_divide_round(
        'recoveries',
        'nullif(tackles_won + interceptions + coalesce(block, 0) + clearances + recoveries, 0)', 3) }} as recovery_rate,

    -- Block Rate
    -- blocks / total_def_actions
    {{ safe_divide_round(
        'coalesce(block, 0)',
        'nullif(tackles_won + interceptions + coalesce(block, 0) + clearances + recoveries, 0)', 3) }} as block_rate,

    -- -------------------------------------------------------------------------
    -- Pass accuracy splits  [NULL where pass maps not scraped]
    -- -------------------------------------------------------------------------

    -- Short Pass Accuracy
    {{ safe_divide_round(
        'coalesce(acc_passes_short, 0)',
        'nullif(passes_short, 0)', 3) }}                                        as acc_rate_short,

    -- Medium Pass Accuracy
    {{ safe_divide_round(
        'coalesce(acc_passes_medium, 0)',
        'nullif(passes_medium, 0)', 3) }}                                       as acc_rate_medium,

    -- Long Pass Accuracy
    {{ safe_divide_round(
        'coalesce(acc_passes_long, 0)',
        'nullif(passes_long, 0)', 3) }}                                         as acc_rate_long,

    -- -------------------------------------------------------------------------
    -- Carry effectiveness  [progressive_carry_distance_pct not available at season level]
    -- -------------------------------------------------------------------------

    -- Carry into Final Third Rate
    -- carries_into_final_third / carries
    {{ safe_divide_round(
        'coalesce(carries_into_final_third, 0)',
        'nullif(carries, 0)', 3) }}                                             as carry_into_final_third_rate,

    -- -------------------------------------------------------------------------
    -- Versatility metrics
    -- -------------------------------------------------------------------------

    -- Duel Winning Ability
    round((
        ((tackles_won + aerial_won + coalesce(dribbles_won, 0))::float
            / nullif(total_tackles + aerial_won + aerial_lost + coalesce(dribbles_total, 0), 0))
        * (tackles_won + aerial_won + coalesce(dribbles_won, 0))
    )::numeric, 3)                                                              as duel_winning_ability,

    -- Passing Range Versatility
    case
        when coalesce(passes_short, 0) + coalesce(passes_medium, 0) + coalesce(passes_long, 0) = 0 then null
        else round((
            1 - (
                abs(passes_short::float / nullif(passes_short + passes_medium + passes_long, 0) - 1.0/3) +
                abs(passes_medium::float / nullif(passes_short + passes_medium + passes_long, 0) - 1.0/3) +
                abs(passes_long::float / nullif(passes_short + passes_medium + passes_long, 0) - 1.0/3)
            ) / (4.0/3)
        )::numeric, 3)
    end                                                                         as passing_range_versatility,

    -- -------------------------------------------------------------------------
    -- Simple rates
    -- -------------------------------------------------------------------------

    -- Shot Accuracy
    {{ safe_divide_round('shots_on_target', 'nullif(total_shots, 0)', 3) }}     as shot_accuracy,

    -- xG per Shot  [NULL where Fotmob crossref not matched]
    {{ safe_divide_round('xg', 'nullif(total_shots, 0)', 3) }}                  as xg_per_shot,

    -- Take-on Success Rate
    {{ safe_divide_round('dribbles_won', 'nullif(dribbles_total, 0)', 3) }}     as take_on_success_rate,

    -- Aerial Win Rate
    {{ safe_divide_round('aerial_won', 'nullif(aerial_won + aerial_lost, 0)', 3) }} as aerial_win_rate,

    -- Tackle Success Rate
    {{ safe_divide_round('tackles_won', 'nullif(total_tackles, 0)', 3) }}       as tackle_success_rate,

    -- Key Pass Rate
    {{ safe_divide_round('key_passes', 'nullif(total_passes, 0)', 3) }}         as key_pass_rate,

    -- Goal Contributions per 90
    {{ safe_divide_round('goals + assists', 'nullif(total_minutes / 90.0, 0)', 3) }} as goal_contributions_per90,

    -- -------------------------------------------------------------------------
    -- Pass direction splits  [NULL where pass maps not scraped]
    -- -------------------------------------------------------------------------

    {{ safe_divide_round('coalesce(passes_forward, 0)',  'nullif(total_passes, 0)', 3) }} as pct_passes_forward,
    {{ safe_divide_round('coalesce(passes_backward, 0)', 'nullif(total_passes, 0)', 3) }} as pct_passes_backward,
    {{ safe_divide_round('coalesce(passes_lateral, 0)',  'nullif(total_passes, 0)', 3) }} as pct_passes_lateral,

    {{ safe_divide_round('coalesce(acc_passes_forward, 0)',  'nullif(passes_forward, 0)',  3) }} as acc_rate_forward,
    {{ safe_divide_round('coalesce(acc_passes_backward, 0)', 'nullif(passes_backward, 0)', 3) }} as acc_rate_backward,
    {{ safe_divide_round('coalesce(acc_passes_lateral, 0)',  'nullif(passes_lateral, 0)',  3) }} as acc_rate_lateral,

    -- -------------------------------------------------------------------------
    -- Progressive pass metrics  [NULL where pass maps not scraped]
    -- -------------------------------------------------------------------------

    {{ safe_divide_round('coalesce(progressive_passes, 0)', 'nullif(total_passes, 0)', 3) }} as progressive_pass_rate,
    {{ safe_divide_round('coalesce(passes_into_final_third, 0)',  'nullif(total_passes, 0)', 3) }} as passes_into_final_third_rate,
    {{ safe_divide_round('coalesce(passes_into_penalty_area, 0)', 'nullif(total_passes, 0)', 3) }} as passes_into_penalty_area_rate,

    -- -------------------------------------------------------------------------
    -- Territory  [NULL where heatmap not scraped]
    -- -------------------------------------------------------------------------

    {{ safe_divide_round(
        'coalesce(att_third_touches, 0)',
        'nullif(coalesce(def_third_touches, 0) + coalesce(mid_third_touches, 0) + coalesce(att_third_touches, 0), 0)', 3) }}
                                                                                as pct_touches_att_third,

    {{ safe_divide_round(
        'coalesce(def_third_touches, 0)',
        'nullif(coalesce(def_third_touches, 0) + coalesce(mid_third_touches, 0) + coalesce(att_third_touches, 0), 0)', 3) }}
                                                                                as pct_touches_def_third

from base
