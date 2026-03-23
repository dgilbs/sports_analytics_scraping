select
    manager,
    fantasy_week                                                                        as week,

    -- Points by active position
    round(cast(sum(case when not is_benched and draft_position = 'GK'
                        then total_points else 0 end) as numeric), 1)                  as pts_gk,
    round(cast(sum(case when not is_benched and draft_position = 'DF'
                        then total_points else 0 end) as numeric), 1)                  as pts_df,
    round(cast(sum(case when not is_benched and draft_position = 'MF'
                        then total_points else 0 end) as numeric), 1)                  as pts_mf,
    round(cast(sum(case when not is_benched and draft_position = 'FW'
                        then total_points else 0 end) as numeric), 1)                  as pts_fw,

    -- Bench points (scored but not counted)
    round(cast(sum(case when is_benched
                        then total_points else 0 end) as numeric), 1)                  as pts_bench,

    -- Total active points
    round(cast(sum(case when not is_benched
                        then total_points else 0 end) as numeric), 1)                  as weekly_points

from {{ ref('fantasy_roster_match_points_2026') }}
group by 1, 2
order by manager, week
