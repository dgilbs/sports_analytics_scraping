with base as (
    select * from {{ ref('player_season_heatmap_stats') }}
),

classified as (
    select
        *,
        case
            -- Goalkeepers
            when position = 'G' then 'GK'

            -- Defenders: central zone distinguishes CB from fullbacks
            when position = 'D' and pct_central >= 0.45
                then 'CB'
            when position = 'D' and pct_left_wing >= pct_right_wing
                then 'LB'
            when position = 'D'
                then 'RB'

            -- Midfielders: wide first, then depth axis
            -- Wide with meaningful attacking presence → winger role
            when position = 'M'
                and greatest(pct_left_wing, pct_right_wing) >= 0.45
                and pct_att_third >= 0.35
                and pct_left_wing >= pct_right_wing
                then 'LM'
            when position = 'M'
                and greatest(pct_left_wing, pct_right_wing) >= 0.45
                and pct_att_third >= 0.35
                then 'RM'
            -- Wide without strong attacking lean → wide mid
            when position = 'M'
                and greatest(pct_left_wing, pct_right_wing) >= 0.50
                and pct_left_wing >= pct_right_wing
                then 'LM'
            when position = 'M'
                and greatest(pct_left_wing, pct_right_wing) >= 0.50
                then 'RM'
            when position = 'M'
                and pct_def_third >= 0.35 and pct_central >= 0.35
                then 'CDM'
            when position = 'M'
                and pct_att_third >= 0.30 and pct_central >= 0.35
                then 'CAM'
            when position = 'M'
                then 'CM'

            -- Forwards: central zone distinguishes striker from wingers
            when position = 'F' and pct_central >= 0.45
                then 'ST'
            when position = 'F' and pct_left_wing >= pct_right_wing
                then 'LW'
            when position = 'F'
                then 'RW'

            else position
        end as position_detailed

    from base
)

select
    player_id,
    player_name,
    season,
    team,
    position               as api_position,
    position_detailed,
    matches_played,
    total_minutes,
    total_touches,
    pct_def_third,
    pct_mid_third,
    pct_att_third,
    pct_left_wing,
    pct_central,
    pct_right_wing

from classified
