-- Calculated analytics metrics per player per match.
-- All metrics pull from player_match_profile which combines Sofascore API,
-- map scrapers (pass/drib/def), heatmap API, and Fotmob xG/xA.
--
-- Notes on proxies / limitations:
--   xg      : Fotmob per-match; NULL where player has no confirmed crossref entry
--   np_goal_efficiency : uses xg as npxg proxy (small distortion, PKs are rare)
--   creative_playmaking_index : excludes xag (not available)
--   shot_quality_creation : uses xa only as proxy for xag + xa
--   ball_security_progression / possession_security_index : uses unsuccessful_touch
--                                                           as carries_miscontrolled proxy
--   map-sourced columns (progressive_passes, carries_into_final_third, etc.)
--                                                         : NULL where match not scraped

with base as (
    select * from {{ ref('player_match_profile') }}
)

select
    event_id,
    season,
    match_date,
    home_team,
    away_team,
    team,
    side,
    player_id,
    player_name,
    position,
    minutes_played,

    -- -------------------------------------------------------------------------
    -- Attacking metrics
    -- -------------------------------------------------------------------------

    -- Complete Attacking Threat
    -- (goals + assists + shots_on_target + key_passes + xg) / touches
    {{ safe_divide_round(
        'goals + assists + shots_on_target + key_passes + coalesce(xg, 0)',
        'nullif(touches, 0)', 3) }}                                             as complete_attacking_threat,

    -- Clinical Finishing
    -- (goals + shots_on_target) / (shots + xg)
    {{ safe_divide_round(
        'goals + shots_on_target',
        'nullif(total_shots + coalesce(xg, 0), 0)', 3) }}                      as clinical_finishing,

    -- Non-Penalty Goal Efficiency  [proxy: xg used as npxg]
    -- (np_goals + shots_on_target) / (shots + xg)
    {{ safe_divide_round(
        'np_goals + shots_on_target',
        'nullif(total_shots + coalesce(xg, 0), 0)', 3) }}                      as np_goal_efficiency,

    -- Final Third Conversion
    -- (shots + key_passes + crosses_into_penalty_area + carries_into_penalty_area) / att_third_touches
    {{ safe_divide_round(
        'total_shots + key_passes + coalesce(crosses_into_penalty_area, 0) + coalesce(carries_into_penalty_area, 0)',
        'nullif(att_third_touches, 0)', 3) }}                                   as final_third_conversion,

    -- Penalty Area Dominance
    -- (att_penalty_area_touches + shots + crosses_into_penalty_area + passes_into_penalty_area) / att_third_touches
    {{ safe_divide_round(
        'coalesce(att_penalty_area_touches, 0) + total_shots + coalesce(crosses_into_penalty_area, 0) + coalesce(passes_into_penalty_area, 0)',
        'nullif(att_third_touches, 0)', 3) }}                                   as penalty_area_dominance,

    -- -------------------------------------------------------------------------
    -- Creative & playmaking metrics
    -- -------------------------------------------------------------------------

    -- Creative Playmaking Index  [excludes xag — not available]
    -- (assists + key_passes + passes_into_penalty_area + crosses_into_penalty_area) / passes_attempted
    {{ safe_divide_round(
        'assists + key_passes + coalesce(passes_into_penalty_area, 0) + coalesce(crosses_into_penalty_area, 0)',
        'nullif(total_pass, 0)', 3) }}                                          as creative_playmaking_index,

    -- Shot Quality Creation  [proxy: xa only, xag not available]
    -- xa / (shots + xg)
    {{ safe_divide_round(
        'coalesce(xa, 0)',
        'nullif(total_shots + coalesce(xg, 0), 0)', 3) }}                      as shot_quality_creation,

    -- -------------------------------------------------------------------------
    -- Defensive metrics
    -- -------------------------------------------------------------------------

    -- Tackling Score
    -- tackles_won_90 × (tackle_success_rate) × (1 + tackles_att / 10)
    round((
        (won_tackle / nullif(minutes_played / 90.0, 0))
        * (won_tackle::float / nullif(total_tackle, 0))
        * (1 + total_tackle / 10.0)
    )::numeric, 3)                                                              as tackling_score,

    -- Defensive Impact Score
    -- (tackles_won + interceptions + blocks + clearances + aerial_won) /
    -- (tackles_att + challenges_att + aerial_won + aerial_lost)
    {{ safe_divide_round(
        'won_tackle + interception_won + coalesce(block, 0) + total_clearance + aerial_won',
        'nullif(total_tackle + total_contest + aerial_won + aerial_lost, 0)', 3) }} as defensive_impact_score,

    -- Defensive Anticipation Index
    -- interceptions / (interceptions + tackles_att)
    {{ safe_divide_round(
        'interception_won',
        'nullif(interception_won + total_tackle, 0)', 3) }}                    as defensive_anticipation_index,

    -- Aerial Dominance Score
    -- (aerial_won / (aerial_won + aerial_lost)) × aerial_won
    round((
        (aerial_won::float / nullif(aerial_won + aerial_lost, 0))
        * aerial_won
    )::numeric, 3)                                                              as aerial_dominance_score,

    -- -------------------------------------------------------------------------
    -- Possession & technical metrics
    -- -------------------------------------------------------------------------

    -- Ball Progression Mastery
    -- (progressive_passes + progressive_carries + passes_into_final_third + carries_into_final_third) /
    -- (passes_attempted + carries)
    {{ safe_divide_round(
        'coalesce(progressive_passes, 0) + progressive_carries_count + coalesce(passes_into_final_third, 0) + coalesce(carries_into_final_third, 0)',
        'nullif(total_pass + carries_count, 0)', 3) }}                          as ball_progression_mastery,

    -- Ball Security & Progression  [proxy: unsuccessful_touch used as carries_miscontrolled]
    -- (passes_completed + take_ons_succeeded + progressive_passes + carries) /
    -- (passes_attempted + take_ons_attempted + carries_miscontrolled + carries_dispossessed)
    {{ safe_divide_round(
        'accurate_pass + won_contest + coalesce(progressive_passes, 0) + carries_count',
        'nullif(total_pass + total_contest + coalesce(unsuccessful_touch, 0) + coalesce(dispossessed, 0), 0)', 3) }} as ball_security_progression,

    -- Possession Security Index  [proxy: unsuccessful_touch used as carries_miscontrolled]
    -- (passes_completed + take_ons_succeeded + carries - carries_miscontrolled) /
    -- (passes_attempted + take_ons_attempted + carries + carries_dispossessed)
    {{ safe_divide_round(
        'accurate_pass + won_contest + carries_count - coalesce(unsuccessful_touch, 0)',
        'nullif(total_pass + total_contest + carries_count + coalesce(dispossessed, 0), 0)', 3) }} as possession_security_index,

    -- Progressive Action Rate
    -- (progressive_passes + progressive_carries + carries_into_final_third + passes_into_final_third) / touches
    {{ safe_divide_round(
        'coalesce(progressive_passes, 0) + progressive_carries_count + coalesce(carries_into_final_third, 0) + coalesce(passes_into_final_third, 0)',
        'nullif(touches, 0)', 3) }}                                             as progressive_action_rate,

    -- Progressive Carrying Impact
    -- (progressive_carries_distance + progressive_carries) / (carries_distance + carries)
    {{ safe_divide_round(
        'progressive_carries_distance + progressive_carries_count',
        'nullif(carries_distance + carries_count, 0)', 3) }}                   as progressive_carrying_impact,

    -- -------------------------------------------------------------------------
    -- Defensive splits
    -- -------------------------------------------------------------------------

    -- Defensive Actions per 90
    -- total_def_actions / (minutes / 90)
    {{ safe_divide_round(
        'won_tackle + interception_won + coalesce(block, 0) + total_clearance + ball_recovery',
        'nullif(minutes_played / 90.0, 0)', 3) }}                               as def_actions_per90,

    -- Recovery Rate
    -- ball_recovery / total_def_actions
    {{ safe_divide_round(
        'ball_recovery',
        'nullif(won_tackle + interception_won + coalesce(block, 0) + total_clearance + ball_recovery, 0)', 3) }} as recovery_rate,

    -- Block Rate
    -- blocks / total_def_actions
    {{ safe_divide_round(
        'coalesce(block, 0)',
        'nullif(won_tackle + interception_won + coalesce(block, 0) + total_clearance + ball_recovery, 0)', 3) }} as block_rate,

    -- -------------------------------------------------------------------------
    -- Pass accuracy splits  [NULL where match not scraped]
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
    -- Carry effectiveness
    -- -------------------------------------------------------------------------

    -- Progressive Carry Distance %
    -- progressive_carries_distance / carries_distance
    {{ safe_divide_round(
        'progressive_carries_distance',
        'nullif(carries_distance, 0)', 3) }}                                    as progressive_carry_distance_pct,

    -- Carry into Final Third Rate
    -- carries_into_final_third / carries
    {{ safe_divide_round(
        'coalesce(carries_into_final_third, 0)',
        'nullif(carries_count, 0)', 3) }}                                       as carry_into_final_third_rate,

    -- -------------------------------------------------------------------------
    -- Versatility metrics
    -- -------------------------------------------------------------------------

    -- Duel Winning Ability
    -- success_rate × volume  (tackles + aerial + take-ons)
    round((
        ((won_tackle + aerial_won + won_contest)::float
            / nullif(total_tackle + aerial_won + aerial_lost + total_contest, 0))
        * (won_tackle + aerial_won + won_contest)
    )::numeric, 3)                                                              as duel_winning_ability,

    -- Passing Range Versatility
    -- 1.0 = perfectly balanced across short / medium / long
    -- 0.0 = all passes in one bucket
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
    {{ safe_divide_round('won_contest', 'nullif(total_contest, 0)', 3) }}       as take_on_success_rate,

    -- Aerial Win Rate
    {{ safe_divide_round('aerial_won', 'nullif(aerial_won + aerial_lost, 0)', 3) }} as aerial_win_rate,

    -- Tackle Success Rate
    {{ safe_divide_round('won_tackle', 'nullif(total_tackle, 0)', 3) }}         as tackle_success_rate,

    -- Key Pass Rate
    {{ safe_divide_round('key_passes', 'nullif(total_pass, 0)', 3) }}           as key_pass_rate,

    -- Goal Contributions per 90
    {{ safe_divide_round('goals + assists', 'nullif(minutes_played / 90.0, 0)', 3) }} as goal_contributions_per90,

    -- -------------------------------------------------------------------------
    -- Pass direction splits  [NULL where match not scraped]
    -- -------------------------------------------------------------------------

    {{ safe_divide_round('coalesce(passes_forward, 0)',  'nullif(total_pass, 0)', 3) }}  as pct_passes_forward,
    {{ safe_divide_round('coalesce(passes_backward, 0)', 'nullif(total_pass, 0)', 3) }}  as pct_passes_backward,
    {{ safe_divide_round('coalesce(passes_lateral, 0)',  'nullif(total_pass, 0)', 3) }}  as pct_passes_lateral,

    {{ safe_divide_round('coalesce(acc_passes_forward, 0)',  'nullif(passes_forward, 0)',  3) }} as acc_rate_forward,
    {{ safe_divide_round('coalesce(acc_passes_backward, 0)', 'nullif(passes_backward, 0)', 3) }} as acc_rate_backward,
    {{ safe_divide_round('coalesce(acc_passes_lateral, 0)',  'nullif(passes_lateral, 0)',  3) }} as acc_rate_lateral,

    -- -------------------------------------------------------------------------
    -- Progressive pass metrics  [NULL where match not scraped]
    -- -------------------------------------------------------------------------

    -- Progressive Pass Rate  (alias of progressive_pass_pct from profile)
    progressive_pass_pct                                                        as progressive_pass_rate,

    {{ safe_divide_round('coalesce(passes_into_final_third, 0)',    'nullif(total_pass, 0)', 3) }} as passes_into_final_third_rate,
    {{ safe_divide_round('coalesce(passes_into_penalty_area, 0)',   'nullif(total_pass, 0)', 3) }} as passes_into_penalty_area_rate,

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
