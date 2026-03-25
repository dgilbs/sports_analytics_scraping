select
    frmp.manager,
    ft.teamname,
    frmp.fantasy_week                                                                        as week,

    -- Points by active position
    round(cast(sum(case when not frmp.is_benched and frmp.draft_position = 'GK'
                        then frmp.total_points else 0 end) as numeric), 1)              as pts_gk,
    round(cast(sum(case when not frmp.is_benched and frmp.draft_position = 'DF'
                        then frmp.total_points else 0 end) as numeric), 1)              as pts_df,
    round(cast(sum(case when not frmp.is_benched and frmp.draft_position = 'MF'
                        then frmp.total_points else 0 end) as numeric), 1)              as pts_mf,
    round(cast(sum(case when not frmp.is_benched and frmp.draft_position = 'FW'
                        then frmp.total_points else 0 end) as numeric), 1)              as pts_fw,

    -- Bench points (scored but not counted)
    round(cast(sum(case when frmp.is_benched
                        then frmp.total_points else 0 end) as numeric), 1)              as pts_bench,

    -- Total active points
    round(cast(sum(case when not frmp.is_benched
                        then frmp.total_points else 0 end) as numeric), 1)              as weekly_points

from {{ ref('fantasy_roster_match_points_2026') }} frmp
join {{ ref('fantasy_teams') }} ft on ft.manager = frmp.manager
group by 1, 2, 3
order by frmp.manager, week
