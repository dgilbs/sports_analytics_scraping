with
stats as (select * from {{ ref('player_season_stats') }}),
pass  as (select * from {{ ref('player_season_pass_stats') }}),
def   as (select * from {{ ref('player_season_def_stats') }}),
drib  as (select * from {{ ref('player_season_drib_stats') }}),
heat  as (select * from {{ ref('player_season_heatmap_stats') }}),
pk    as (select * from {{ ref('player_season_pk_stats') }}),

fotmob as (
    select
        px.sofascore_player_id as player_id,
        fxg.season,
        sum(fxg.xg) as xg,
        sum(fxg.xa) as xa
    from {{ source('fotmob', 'player_match_stats') }} fxg
    join {{ source('sofascore', 'sofascore_fotmob_match_xref') }}  mx on fxg.match_id  = mx.fotmob_match_id
    join {{ source('sofascore', 'sofascore_fotmob_crossref') }}    px on fxg.player_id = px.fotmob_player_id
    where px.needs_review = false
    group by px.sofascore_player_id, fxg.season
)

select
    -- Identity (API)
    s.player_id,
    s.player_name,
    s.season,
    s.team,
    s.position,
    s.matches_played,
    s.total_minutes,
    s.avg_rating,
    s.avg_rating_alternative,

    -- Goals & Attacking (API)
    s.goals,
    coalesce(pk.pk_goals,    0)                         as pk_goals,
    coalesce(pk.pk_attempts, 0)                         as pk_attempts,
    coalesce(pk.pk_missed,   0)                         as pk_missed,
    -- calculated: goals excluding penalties
    s.goals - coalesce(pk.pk_goals, 0)                  as np_goals,
    s.assists,
    s.key_passes,
    s.total_shots,
    s.shots_on_target,
    s.big_chances_missed,
    s.shot_accuracy,
    s.shot_conversion,

    -- Passing counts (API)
    s.total_passes,
    s.accurate_passes,
    s.total_long_balls,
    s.accurate_long_balls,
    s.total_crosses,
    s.accurate_crosses,
    s.pass_completion,
    s.long_ball_completion,
    s.cross_completion,

    -- Passing extras (map only)
    ps.avg_pass_length,
    ps.progressive_passes,
    ps.progressive_pass_pct,
    ps.passes_forward,
    ps.passes_backward,
    ps.passes_lateral,
    ps.acc_passes_forward,
    ps.acc_passes_backward,
    ps.acc_passes_lateral,
    ps.origin_def_third,
    ps.origin_mid_third,
    ps.origin_att_third,
    ps.origin_left_wing,
    ps.origin_central,
    ps.origin_right_wing,
    ps.dest_def_third,
    ps.dest_mid_third,
    ps.dest_att_third,
    ps.dest_left_wing,
    ps.dest_central,
    ps.dest_right_wing,
    ps.acc_dest_def_third,
    ps.acc_dest_mid_third,
    ps.acc_dest_att_third,
    ps.acc_dest_left_wing,
    ps.acc_dest_central,
    ps.acc_dest_right_wing,
    ps.passes_into_final_third,
    ps.acc_passes_into_final_third,
    ps.passes_into_penalty_area,
    ps.acc_passes_into_penalty_area,
    ps.crosses_into_penalty_area,
    ps.passes_short,
    ps.passes_medium,
    ps.passes_long,
    ps.acc_passes_short,
    ps.acc_passes_medium,
    ps.acc_passes_long,

    -- Carries & progression (API)
    s.carries,
    s.progressive_carries,
    s.total_progression,
    s.progressive_carry_rate,

    -- Touches & possession (API)
    s.touches,
    s.possession_lost,

    -- Duels (API)
    s.duels_won,
    s.duels_lost,
    s.aerial_won,
    s.aerial_lost,
    s.duel_win_rate,
    s.aerial_win_rate,

    -- Dribbles (map only — not separately tracked in API season model)
    dr.dribbles_won,
    dr.dribbles_lost,
    dr.dribbles_total,
    dr.dribble_success,

    -- Dribble & carry zones (map only)
    dr.drib_def_third,
    dr.drib_mid_third,
    dr.drib_att_third,
    dr.drib_left_wing,
    dr.drib_central,
    dr.drib_right_wing,
    dr.drib_won_def_third,
    dr.drib_won_mid_third,
    dr.drib_won_att_third,
    dr.carry_segments,
    dr.carry_def_third,
    dr.carry_mid_third,
    dr.carry_att_third,
    dr.carry_left_wing,
    dr.carry_central,
    dr.carry_right_wing,
    dr.carries_into_final_third,
    dr.carries_into_penalty_area,

    -- Defending (API)
    s.total_tackles,
    s.tackles_won,
    s.interceptions,
    s.clearances,
    s.recoveries,
    s.tackle_success_rate,

    -- Defending extras (map only)
    df.block,
    df.total_def_actions,
    df.def_actions_def_third,
    df.def_actions_mid_third,
    df.def_actions_att_third,
    df.def_actions_left_wing,
    df.def_actions_central,
    df.def_actions_right_wing,
    df.tackle_won_def_third,
    df.tackle_won_mid_third,
    df.tackle_won_att_third,
    df.intercept_def_third,
    df.intercept_mid_third,
    df.intercept_att_third,
    df.recovery_def_third,
    df.recovery_mid_third,
    df.recovery_att_third,

    -- Discipline (API)
    s.fouls,
    s.was_fouled,

    -- Per 90 (API — from player_season_stats)
    s.goals_per90,
    s.assists_per90,
    s.key_passes_per90,
    s.shots_per90,
    s.interceptions_per90,
    s.tackles_per90,
    s.recoveries_per90,
    s.carries_per90,

    -- Heatmap touches by zone
    heat.total_touches,
    heat.def_third_touches,
    heat.mid_third_touches,
    heat.att_third_touches,
    heat.left_wing_touches,
    heat.central_touches,
    heat.right_wing_touches,
    heat.pct_def_third,
    heat.pct_mid_third,
    heat.pct_att_third,
    heat.pct_left_wing,
    heat.pct_central,
    heat.pct_right_wing,
    heat.att_penalty_area_touches,
    heat.pct_att_penalty_area,

    -- xG / xA (Fotmob, where matched)
    fxg.xg,
    fxg.xa

from stats s
left join pass   ps  on s.player_id = ps.player_id and s.season = ps.season and s.team = ps.team and s.position = ps.position
left join def    df  on s.player_id = df.player_id and s.season = df.season and s.team = df.team and s.position = df.position
left join drib   dr  on s.player_id = dr.player_id and s.season = dr.season and s.team = dr.team and s.position = dr.position
left join heat  heat on s.player_id = heat.player_id and s.season = heat.season and s.team = heat.team and s.position = heat.position
left join fotmob fxg on s.player_id = fxg.player_id and s.season = fxg.season
left join pk         on s.player_id = pk.player_id  and s.season = pk.season
