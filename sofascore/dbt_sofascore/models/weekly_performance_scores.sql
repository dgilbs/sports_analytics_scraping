-- Weekly performance scoring engine for women's soccer analytics publication.
-- Ranks every player-match appearance by notability, combining:
--   1. Position-adjusted composite value (shot/pass/dribble/defensive values, weighted by role)
--   2. Custom metrics bonus from player_match_metrics (capped at 3.0 to keep additive)
--   3. Rating delta vs. player's own season average
--   4. Contextual bonus flags (brace, goal+assist, defensive standout, etc.)
--   5. Penalty flags for underperformers
--
-- Minimum 45 minutes played to qualify.
-- Intended usage: filter by match_date range, order by notability_score DESC, take top N.
--
-- Notes:
--   custom_metrics_bonus is capped at 3.0 across all positions to prevent
--   unbounded metrics (e.g. defensive_impact_score, duel_winning_ability)
--   from dominating the score.
--   Season baseline requires >= 3 appearances; players with fewer matches
--   get a rating_delta of 0 (no penalty/bonus for small sample).

with match_base as (
    select
        p.event_id,
        p.player_id,
        p.player_name,
        p.team,
        p.position,
        p.side,
        p.match_date,
        p.home_team,
        p.away_team,
        p.minutes_played,
        p.season,
        p.rating,
        p.goals,
        p.assists,
        p.key_passes,
        p.interception_won,
        p.duel_won,
        p.duel_lost,
        p.shot_value,
        p.pass_value,
        p.dribble_value,
        p.defensive_value,
        p.total_value,
        -- Custom metrics from player_match_metrics
        m.complete_attacking_threat,
        m.clinical_finishing,
        m.np_goal_efficiency,
        m.final_third_conversion,
        m.penalty_area_dominance,
        m.creative_playmaking_index,
        m.shot_quality_creation,
        m.tackling_score,
        m.defensive_impact_score,
        m.defensive_anticipation_index,
        m.aerial_dominance_score,
        m.ball_progression_mastery,
        m.ball_security_progression,
        m.possession_security_index,
        m.progressive_action_rate,
        m.progressive_carrying_impact,
        m.def_actions_per90,
        m.recovery_rate,
        m.block_rate,
        m.acc_rate_short,
        m.acc_rate_medium,
        m.acc_rate_long,
        m.progressive_carry_distance_pct,
        m.carry_into_final_third_rate,
        m.duel_winning_ability,
        m.passing_range_versatility,
        m.shot_accuracy,
        m.xg_per_shot,
        m.take_on_success_rate,
        m.aerial_win_rate,
        m.tackle_success_rate,
        m.key_pass_rate,
        m.goal_contributions_per90,
        m.pct_passes_forward,
        m.pct_passes_backward,
        m.pct_passes_lateral,
        m.acc_rate_forward,
        m.acc_rate_backward,
        m.acc_rate_lateral,
        m.progressive_pass_rate,
        m.passes_into_final_third_rate,
        m.passes_into_penalty_area_rate,
        m.pct_touches_att_third,
        m.pct_touches_def_third
    from {{ ref('player_match_profile') }} p
    left join {{ ref('player_match_metrics') }} m
        on p.event_id = m.event_id and p.player_id = m.player_id
    where p.minutes_played >= 45
),

season_baselines as (
    select
        player_id,
        season,
        avg_rating,
        matches_played
    from {{ ref('player_season_stats') }}
    where matches_played >= 3
),

scored as (
    select
        b.*,
        bl.avg_rating     as season_avg_rating,
        bl.matches_played as season_matches,

        -- Rating delta vs season average (0 if no baseline yet)
        round((b.rating - coalesce(bl.avg_rating, b.rating))::numeric, 2) as rating_delta,

        -- Position-adjusted raw value
        round(case
            when b.position = 'F' then
                coalesce(b.shot_value, 0) * 1.5 + coalesce(b.pass_value, 0) * 0.8
                + coalesce(b.dribble_value, 0) * 1.2 + coalesce(b.defensive_value, 0) * 0.5
            when b.position = 'M' then
                coalesce(b.shot_value, 0) * 1.0 + coalesce(b.pass_value, 0) * 1.2
                + coalesce(b.dribble_value, 0) * 1.0 + coalesce(b.defensive_value, 0) * 0.8
            when b.position = 'D' then
                coalesce(b.shot_value, 0) * 0.5 + coalesce(b.pass_value, 0) * 1.2
                + coalesce(b.dribble_value, 0) * 0.8 + coalesce(b.defensive_value, 0) * 1.5
            when b.position = 'G' then
                coalesce(b.pass_value, 0) * 1.0 + coalesce(b.defensive_value, 0) * 2.0
            else b.total_value
        end::numeric, 3) as position_adj_value,

        -- Position-adjusted custom metrics bonus, capped at 3.0
        round(least(case
            when b.position = 'F' then
                coalesce(b.complete_attacking_threat, 0) * 2.0
                + coalesce(b.clinical_finishing, 0) * 1.5
                + coalesce(b.final_third_conversion, 0) * 1.5
                + coalesce(b.progressive_carrying_impact, 0) * 1.0
                + coalesce(b.xg_per_shot, 0) * 1.0
            when b.position = 'M' then
                coalesce(b.creative_playmaking_index, 0) * 2.0
                + coalesce(b.ball_progression_mastery, 0) * 1.5
                + coalesce(b.progressive_action_rate, 0) * 1.5
                + coalesce(b.complete_attacking_threat, 0) * 1.0
                + coalesce(b.possession_security_index, 0) * 1.0
                + coalesce(b.defensive_impact_score, 0) * 0.5
            when b.position = 'D' then
                coalesce(b.defensive_impact_score, 0) * 2.0
                + coalesce(b.ball_progression_mastery, 0) * 1.5
                + coalesce(b.defensive_anticipation_index, 0) * 1.5
                + coalesce(b.possession_security_index, 0) * 1.0
                + coalesce(b.aerial_dominance_score, 0) * 0.5
            when b.position = 'G' then
                coalesce(b.defensive_impact_score, 0) * 2.0
                + coalesce(b.possession_security_index, 0) * 1.5
            else 0
        end, 3.0)::numeric, 3) as custom_metrics_bonus,

        -- Contextual bonus flags
        case when b.goals >= 2 then 1 else 0 end                                               as flag_brace,
        case when b.goals >= 1 and b.assists >= 1 then 1 else 0 end                            as flag_goal_and_assist,
        case when b.key_passes >= 4 then 1 else 0 end                                          as flag_creative_standout,
        case when b.position in ('D','G') and b.defensive_value >= 0.5 then 1 else 0 end       as flag_defensive_standout,
        case when b.position = 'D' and b.goals >= 1 then 1 else 0 end                          as flag_defender_scored,
        case when b.interception_won >= 4 then 1 else 0 end                                     as flag_interception_machine,
        case when b.duel_won >= 8 then 1 else 0 end                                             as flag_duel_monster,
        case when b.rating >= 8.5 then 1 else 0 end                                             as flag_elite_rating,
        case when b.complete_attacking_threat >= 0.3 then 1 else 0 end                          as flag_attacking_threat,
        case when b.ball_progression_mastery >= 0.5 then 1 else 0 end                           as flag_progressive_carrier,
        case when b.defensive_impact_score >= 0.6 then 1 else 0 end                             as flag_defensive_impact,
        case when b.duel_winning_ability >= 3.0 then 1 else 0 end                               as flag_duel_dominator,
        case when b.xg_per_shot >= 0.2 then 1 else 0 end                                        as flag_high_xg_shots,
        -- Negative flags
        case when bl.avg_rating is not null and b.rating < bl.avg_rating - 1.0 then 1 else 0 end as flag_underperformer,
        case when b.rating < 6.5 then 1 else 0 end                                              as flag_poor_match

    from match_base b
    left join season_baselines bl on b.player_id = bl.player_id and b.season = bl.season
),

final_scored as (
    select
        *,

        -- Overall notability score
        round((
            position_adj_value * 2.0
            + custom_metrics_bonus * 0.5
            + coalesce(rating_delta, 0) * 1.5
            + flag_brace * 2.0
            + flag_goal_and_assist * 1.5
            + flag_creative_standout * 1.0
            + flag_defensive_standout * 1.5
            + flag_defender_scored * 1.0
            + flag_interception_machine * 1.0
            + flag_duel_monster * 0.5
            + flag_elite_rating * 1.0
            + flag_attacking_threat * 0.5
            + flag_progressive_carrier * 0.5
            + flag_defensive_impact * 0.5
            + flag_duel_dominator * 0.5
            + flag_high_xg_shots * 0.5
            - flag_underperformer * 2.0
            - flag_poor_match * 1.5
        )::numeric, 3) as notability_score,

        -- Primary narrative type (highest priority flag wins)
        case
            when flag_brace = 1                 then 'Clinical Finisher'
            when flag_goal_and_assist = 1        then 'Goal Contributor'
            when flag_defensive_standout = 1     then 'Defensive Standout'
            when flag_defender_scored = 1        then 'Attacking Defender'
            when flag_creative_standout = 1      then 'Creative Engine'
            when flag_interception_machine = 1   then 'Defensive Standout'
            when flag_duel_dominator = 1         then 'Physical Dominator'
            when flag_duel_monster = 1           then 'Physical Dominator'
            when flag_progressive_carrier = 1    then 'Progressive Force'
            when flag_attacking_threat = 1       then 'Attacking Threat'
            when flag_elite_rating = 1           then 'All-Round Performer'
            when flag_underperformer = 1         then 'Underperformer'
            when flag_poor_match = 1             then 'Poor Performance'
            else 'Solid Performer'
        end as narrative_type

    from scored
)

select * from final_scored
order by notability_score desc
