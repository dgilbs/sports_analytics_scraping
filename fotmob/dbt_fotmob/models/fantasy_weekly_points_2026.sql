with roster_history as (
    -- Point-in-time roster: use snapshot for ownership history.
    -- For original draft picks (present in nwsfl_rosters_2026), backdate valid_from
    -- to season start so late-resolved IDs don't miss early weeks.
    -- For mid-season additions (trades/pickups), use the actual snapshot date.
    select
        s.manager,
        s.player                                as roster_name,
        s.db_name                               as db_name,
        nullif(s.player_id::text, '')::bigint   as player_id,
        case
            when d."Player" is not null then '2026-03-12'::date
            else min(s.dbt_valid_from::date) over (partition by s.manager, s.player)
        end                                     as valid_from,
        s.dbt_valid_to::date                    as valid_to
    from {{ ref('fantasy_roster_2026_snapshot') }} s
    left join {{ ref('nwsfl_rosters_2026') }} d
        on s.manager = d."Manager"
        and s.player = d."Player"
    where nullif(s.player_id::text, '') is not null
),

bench_decisions as (
    select
        week::integer        as week,
        manager::text        as manager,
        benched_player::text as benched_player
    from {{ ref('fantasy_weekly_benches_2026') }}
),

fantasy_weeks as (
    select * from {{ ref('fantasy_weeks_2026') }}
),

benched_player_ids as (
    -- Resolve benched player names to player_ids using the roster active at the start of that week
    select
        b.week,
        b.manager,
        r.player_id as benched_player_id
    from bench_decisions b
    join fantasy_weeks fw on b.week = fw.week::integer
    join roster_history r
        on b.manager = r.manager
        and (b.benched_player = r.roster_name or b.benched_player = r.db_name)
        and fw.week_start >= r.valid_from
        and (r.valid_to is null or fw.week_start < r.valid_to)
),

match_points as (
    select
        mp.*,
        fw.week as fantasy_week
    from {{ ref('fantasy_match_points') }} mp
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
    join roster_history r
        on mp.player_id = r.player_id
        and mp.match_date::date >= r.valid_from
        and (r.valid_to is null or mp.match_date::date < r.valid_to)
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
