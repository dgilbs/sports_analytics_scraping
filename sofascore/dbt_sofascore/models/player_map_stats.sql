-- Joins pass, drib, and def map counts into a single per-player-per-match view.
-- Includes only players who have data in at least one map table.
-- All zone columns use estimated counts (_countsuffix), not raw percentages.

with pass as (
    select * from {{ ref('pass_map_counts') }}
),

drib as (
    select * from {{ ref('drib_map_counts') }}
),

def as (
    select * from {{ ref('def_map_counts') }}
),

spine as (
    select event_id, player_id from pass
    union
    select event_id, player_id from drib
    union
    select event_id, player_id from def
)

select
    s.event_id,
    s.player_id,
    coalesce(pass.season,      drib.season,      def.season)      as season,
    coalesce(pass.match_date,  drib.match_date,  def.match_date)  as match_date,
    coalesce(pass.home_team,   drib.home_team,   def.home_team)   as home_team,
    coalesce(pass.away_team,   drib.away_team,   def.away_team)   as away_team,
    coalesce(pass.team,        drib.team,        def.team)        as team,
    coalesce(pass.side,        drib.side,        def.side)        as side,
    coalesce(pass.player_name, drib.player_name, def.player_name) as player_name,
    coalesce(pass.position,    drib.position,    def.position)    as position,
    coalesce(pass.substitute,  drib.substitute,  def.substitute)  as substitute,

    -- ── Pass ──────────────────────────────────────────────────────────────────
    pass.passes_total,
    pass.passes_accurate,
    pass.passes_inaccurate,
    pass.pass_accuracy,
    pass.avg_pass_length,
    pass.progressive_passes,
    pass.progressive_pass_pct,
    pass.passes_forward,
    pass.passes_backward,
    pass.passes_lateral,
    pass.acc_passes_forward,
    pass.origin_def_third_count     as pass_origin_def_third,
    pass.origin_mid_third_count     as pass_origin_mid_third,
    pass.origin_att_third_count     as pass_origin_att_third,
    pass.dest_def_third_count       as pass_dest_def_third,
    pass.dest_mid_third_count       as pass_dest_mid_third,
    pass.dest_att_third_count       as pass_dest_att_third,
    pass.acc_dest_att_third_count   as acc_pass_dest_att_third,

    -- ── Dribble / Carry ───────────────────────────────────────────────────────
    drib.dribbles_won,
    drib.dribbles_lost,
    drib.dribbles_total,
    drib.dribble_success,
    drib.carry_segments,
    drib.drib_def_third_count       as drib_def_third,
    drib.drib_mid_third_count       as drib_mid_third,
    drib.drib_att_third_count       as drib_att_third,
    drib.drib_won_def_third_count   as drib_won_def_third,
    drib.drib_won_mid_third_count   as drib_won_mid_third,
    drib.drib_won_att_third_count   as drib_won_att_third,
    drib.carry_def_third_count      as carry_def_third,
    drib.carry_mid_third_count      as carry_mid_third,
    drib.carry_att_third_count      as carry_att_third,

    -- ── Defense ───────────────────────────────────────────────────────────────
    def.tackle_won,
    def.missed_tackle,
    def.interception,
    def.clearance,
    def.block,
    def.recovery,
    def.total_def_actions,
    def.tackle_success,
    def.def_actions_def_third_count as def_actions_def_third,
    def.def_actions_mid_third_count as def_actions_mid_third,
    def.def_actions_att_third_count as def_actions_att_third,
    def.tackle_won_def_third_count  as tackle_def_third,
    def.tackle_won_mid_third_count  as tackle_mid_third,
    def.tackle_won_att_third_count  as tackle_att_third,
    def.intercept_def_third_count   as intercept_def_third,
    def.intercept_mid_third_count   as intercept_mid_third,
    def.intercept_att_third_count   as intercept_att_third,
    def.recovery_def_third_count    as recovery_def_third,
    def.recovery_mid_third_count    as recovery_mid_third,
    def.recovery_att_third_count    as recovery_att_third

from spine s
left join pass on s.event_id = pass.event_id and s.player_id = pass.player_id
left join drib on s.event_id = drib.event_id and s.player_id = drib.player_id
left join def  on s.event_id = def.event_id  and s.player_id = def.player_id
