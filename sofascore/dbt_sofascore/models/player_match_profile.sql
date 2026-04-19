with
stats as (select * from {{ ref('player_match_stats') }}),
pass  as (select * from {{ ref('pass_map_counts') }}),
def   as (select * from {{ ref('def_map_counts') }}),
drib  as (select * from {{ ref('drib_map_counts') }}),
heat  as (select * from {{ ref('heatmap_counts') }}),
pk    as (select * from {{ ref('player_match_pk_stats') }}),

fotmob as (
    select
        mx.sofascore_event_id  as event_id,
        px.sofascore_player_id as player_id,
        fxg.xg,
        fxg.xa
    from {{ source('fotmob', 'player_match_stats') }} fxg
    join {{ source('sofascore', 'sofascore_fotmob_match_xref') }}  mx on fxg.match_id  = mx.fotmob_match_id
    join {{ source('sofascore', 'sofascore_fotmob_crossref') }}    px on fxg.player_id = px.fotmob_player_id
    where px.needs_review = false
)

select
    -- -------------------------------------------------------------------------
    -- Identity & context
    -- source: Sofascore API + dim_matches join
    -- -------------------------------------------------------------------------
    s.event_id,
    s.season,
    s.match_date,
    s.home_team,
    s.away_team,
    s.team,
    s.side,
    s.player_id,
    s.player_name,
    s.position,
    s.substitute,
    s.minutes_played,
    s.rating,               -- Sofascore match rating (0–10)
    s.rating_alternative,   -- Sofascore alternative rating

    -- calculated: derived from dim_matches.winner_code + player side
    s.result,
    s.home_score,
    s.away_score,
    s.goals_conceded,

    -- -------------------------------------------------------------------------
    -- Goals & Attacking
    -- source: Sofascore API (raw counts)
    -- -------------------------------------------------------------------------
    s.goals,
    s.assists,
    s.key_passes,
    s.total_shots,
    s.shots_on_target,
    s.shots_off_target,
    s.shots_blocked,
    s.big_chance_missed,
    s.total_offside,

    -- Penalty kicks (source: fact_shots, situation = 'penalty')
    coalesce(pk.pk_attempts, 0)                         as pk_attempts,
    coalesce(pk.pk_goals,    0)                         as pk_goals,
    coalesce(pk.pk_missed,   0)                         as pk_missed,
    -- calculated: goals excluding penalties
    s.goals - coalesce(pk.pk_goals, 0)                  as np_goals,

    -- calculated: shots_on_target / total_shots, goals / total_shots
    s.shot_accuracy,
    s.shot_conversion,

    -- -------------------------------------------------------------------------
    -- Passing
    -- source: Sofascore API (raw counts)
    -- -------------------------------------------------------------------------
    s.total_pass,
    s.accurate_pass,
    s.total_long_balls,
    s.accurate_long_balls,
    s.total_cross,
    s.accurate_cross,
    s.own_half_passes,
    s.accurate_own_half_passes,
    s.opp_half_passes,
    s.accurate_opp_half_passes,

    -- calculated: accurate / total for each pass type
    s.pass_completion,
    s.long_ball_completion,
    s.cross_completion,
    s.opp_half_pass_completion,

    -- -------------------------------------------------------------------------
    -- Passing — spatial breakdown
    -- source: Sofascore pass map (Playwright scraper, SVG coordinates)
    -- -------------------------------------------------------------------------
    p.avg_pass_length,
    p.progressive_passes,
    p.progressive_pass_pct,    -- calculated: progressive_passes / total_pass
    p.passes_forward,
    p.passes_backward,
    p.passes_lateral,
    p.acc_passes_forward,
    p.acc_passes_backward,
    p.acc_passes_lateral,
    p.origin_def_third_count,
    p.origin_mid_third_count,
    p.origin_att_third_count,
    p.origin_left_wing_count,
    p.origin_central_count,
    p.origin_right_wing_count,
    p.dest_def_third_count,
    p.dest_mid_third_count,
    p.dest_att_third_count,
    p.dest_left_wing_count,
    p.dest_central_count,
    p.dest_right_wing_count,
    p.acc_dest_def_third_count,
    p.acc_dest_mid_third_count,
    p.acc_dest_att_third_count,
    p.acc_dest_left_wing_count,
    p.acc_dest_central_count,
    p.acc_dest_right_wing_count,
    p.passes_into_final_third,
    p.acc_passes_into_final_third,
    p.passes_into_penalty_area,
    p.acc_passes_into_penalty_area,
    p.crosses_into_penalty_area,
    p.passes_short,
    p.passes_medium,
    p.passes_long,
    p.acc_passes_short,
    p.acc_passes_medium,
    p.acc_passes_long,

    -- -------------------------------------------------------------------------
    -- Carries & Progression
    -- source: Sofascore API (raw counts)
    -- -------------------------------------------------------------------------
    s.carries_count,
    s.carries_distance,
    s.progressive_carries_count,
    s.progressive_carries_distance,
    s.total_progression,
    s.best_carry_progression,

    -- calculated: progressive_carries_count / carries_count
    s.progressive_carry_rate,

    -- -------------------------------------------------------------------------
    -- Dribbles & Carries — spatial breakdown
    -- source: Sofascore dribble map (Playwright scraper, SVG coordinates)
    -- -------------------------------------------------------------------------
    d.drib_def_third_count,
    d.drib_mid_third_count,
    d.drib_att_third_count,
    d.drib_left_wing_count,
    d.drib_central_count,
    d.drib_right_wing_count,
    d.drib_won_def_third_count,
    d.drib_won_mid_third_count,
    d.drib_won_att_third_count,
    d.carry_def_third_count,
    d.carry_mid_third_count,
    d.carry_att_third_count,
    d.carry_left_wing_count,
    d.carry_central_count,
    d.carry_right_wing_count,
    d.carries_into_final_third,
    d.carries_into_penalty_area,

    -- -------------------------------------------------------------------------
    -- Touches & Possession
    -- source: Sofascore API (raw counts)
    -- -------------------------------------------------------------------------
    s.touches,
    s.unsuccessful_touch,
    s.possession_lost,
    s.dispossessed,

    -- -------------------------------------------------------------------------
    -- Duels
    -- source: Sofascore API (raw counts)
    -- -------------------------------------------------------------------------
    s.duel_won,
    s.duel_lost,
    s.aerial_won,
    s.aerial_lost,

    -- calculated: won / (won + lost)
    s.duel_win_rate,
    s.aerial_win_rate,

    -- -------------------------------------------------------------------------
    -- Dribbles (contest outcomes)
    -- source: Sofascore API (raw counts)
    -- -------------------------------------------------------------------------
    s.total_contest,
    s.won_contest,
    s.challenge_lost,

    -- calculated: won_contest / total_contest
    s.contest_win_rate,

    -- -------------------------------------------------------------------------
    -- Defending
    -- source: Sofascore API (raw counts)
    -- -------------------------------------------------------------------------
    s.total_tackle,
    s.won_tackle,
    s.interception_won,
    s.total_clearance,
    s.ball_recovery,

    -- calculated: won_tackle / total_tackle
    s.tackle_success_rate,

    -- -------------------------------------------------------------------------
    -- Defending — spatial breakdown
    -- source: Sofascore defensive map (Playwright scraper, SVG coordinates)
    -- -------------------------------------------------------------------------
    df.block,
    df.total_def_actions,
    df.def_actions_def_third_count,
    df.def_actions_mid_third_count,
    df.def_actions_att_third_count,
    df.def_actions_left_wing_count,
    df.def_actions_central_count,
    df.def_actions_right_wing_count,
    df.tackle_won_def_third_count,
    df.tackle_won_mid_third_count,
    df.tackle_won_att_third_count,
    df.intercept_def_third_count,
    df.intercept_mid_third_count,
    df.intercept_att_third_count,
    df.recovery_def_third_count,
    df.recovery_mid_third_count,
    df.recovery_att_third_count,

    -- -------------------------------------------------------------------------
    -- Discipline & composite value scores
    -- source: Sofascore API
    -- note: *_value are Sofascore's proprietary normalized composite scores
    --       (roughly -1 to +1) reflecting contribution per phase of play
    -- -------------------------------------------------------------------------
    s.fouls,
    s.was_fouled,
    s.shot_value,
    s.pass_value,
    s.dribble_value,
    s.defensive_value,
    coalesce(s.shot_value, 0) + coalesce(s.pass_value, 0) + coalesce(s.dribble_value, 0) + coalesce(s.defensive_value, 0) as total_value,

    -- -------------------------------------------------------------------------
    -- Heatmap — touch counts by zone
    -- source: Sofascore heatmap API
    -- -------------------------------------------------------------------------
    heat.touch_count,
    heat.def_third_touches,
    heat.mid_third_touches,
    heat.att_third_touches,
    heat.left_wing_touches,
    heat.central_touches,
    heat.right_wing_touches,
    heat.att_penalty_area_touches,

    -- -------------------------------------------------------------------------
    -- Expected goals / assists
    -- source: Fotmob (joined via sofascore_fotmob_match_xref + sofascore_fotmob_crossref)
    -- note: NULL where player or match has no confirmed crossref entry
    -- -------------------------------------------------------------------------
    fxg.xg,
    fxg.xa

from stats s
left join pass   p   on s.event_id = p.event_id    and s.player_id = p.player_id
left join def    df  on s.event_id = df.event_id   and s.player_id = df.player_id
left join drib   d   on s.event_id = d.event_id    and s.player_id = d.player_id
left join heat  heat on s.event_id = heat.event_id and s.player_id = heat.player_id
left join fotmob fxg on s.event_id = fxg.event_id  and s.player_id = fxg.player_id
left join pk         on s.event_id = pk.event_id   and s.player_id = pk.player_id
