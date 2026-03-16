with roster as (
    -- Manager → player_id mapping from the 2026 matched roster seed
    select
        manager,
        player                      as roster_name,
        nullif(player_id::text, '')::bigint as player_id
    from "neondb"."fotmob"."nwsfl_roster_matched"
    where nullif(player_id::text, '') is not null
),

bench_decisions as (
    select
        week::integer        as week,
        manager::text        as manager,
        benched_player::text as benched_player
    from "neondb"."fotmob"."fantasy_weekly_benches_2026"
),

benched_player_ids as (
    -- Resolve benched player names to player_ids via roster
    select
        b.week,
        b.manager,
        r.player_id as benched_player_id
    from bench_decisions b
    join roster r
        on b.manager = r.manager
        and b.benched_player = r.roster_name
),

fantasy_weeks as (
    select * from "neondb"."fotmob"."fantasy_weeks_2026"
),

match_points as (
    select
        mp.*,
        fw.week as fantasy_week
    from "neondb"."fotmob"."fantasy_match_points" mp
    join fantasy_weeks fw
        on mp.match_date::date between fw.week_start and fw.week_end
),

-- Attach manager to each match-point row; mark benched players
roster_match_points as (
    select
        mp.match_id,
        mp.player_id,
        mp.player_name,
        mp.match_date,
        mp.fantasy_week,
        mp.team_name,
        mp.draft_position,
        mp.minutes_played,
        mp.total_points,
        r.manager,
        case
            when b.benched_player_id is not null then true
            else false
        end as is_benched
    from match_points mp
    join roster r
        on mp.player_id = r.player_id
    left join benched_player_ids b
        on mp.fantasy_week = b.week
        and r.manager = b.manager
        and mp.player_id = b.benched_player_id
)

select
    manager,
    fantasy_week                                                        as week,
    count(distinct player_id) filter (where not is_benched)            as active_players,
    count(distinct player_id) filter (where is_benched)                as benched_players,
    round(cast(
        sum(case when not is_benched then total_points else 0 end)
    as numeric), 1)                                                     as weekly_points
from roster_match_points
group by 1, 2
order by manager, week