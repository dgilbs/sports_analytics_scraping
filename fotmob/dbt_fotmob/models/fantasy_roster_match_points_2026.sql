with transfers as (
    select
        transfer_date::date  as transfer_date,
        manager,
        player_out,
        player_in
    from {{ ref('transfers_2026') }}
),

roster_history as (
    -- Initial roster: valid from season start, valid_to = first transfer out date (if any)
    select
        r.manager,
        r.player                                as roster_name,
        r.db_name,
        r.player_id::bigint                     as player_id,
        '2026-03-12'::date                      as valid_from,
        min(t.transfer_date)                    as valid_to
    from {{ ref('nwsfl_roster_matched') }} r
    left join transfers t
        on t.manager = r.manager
        and t.player_out = r.player
    where not exists (
        select 1 from transfers ti
        where ti.manager = r.manager
        and ti.player_in = r.player
    )
    group by r.manager, r.player, r.db_name, r.player_id

    union all

    -- Transferred-in players: valid from transfer date, valid_to = next transfer out (if any)
    select
        t.manager,
        t.player_in                             as roster_name,
        m.db_name,
        m.player_id::bigint                     as player_id,
        t.transfer_date                         as valid_from,
        min(t2.transfer_date)                   as valid_to
    from transfers t
    join {{ ref('player_id_mapping') }} m on t.player_in = m.seed_name
    left join transfers t2
        on t2.manager = t.manager
        and t2.player_out = t.player_in
        and t2.transfer_date > t.transfer_date
    group by t.manager, t.player_in, m.db_name, m.player_id, t.transfer_date
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
    select * from {{ ref('fantasy_match_points') }}
),

-- Most recent team per player (across all seasons)
player_teams as (
    select distinct on (player_id)
        player_id,
        team_id
    from {{ ref('player_match_stats') }}
    order by player_id, match_date desc
),

-- Matches that have actually been played (stats exist)
played_matches as (
    select distinct match_id
    from {{ ref('player_match_stats') }}
),

-- All team-match combinations per fantasy week for completed matches
team_week_matches as (
    select
        m.match_id,
        m.utc_time::date    as match_date,
        fw.week             as fantasy_week,
        m.home_team_id      as team_id
    from {{ source('fotmob', 'dim_matches') }} m
    join fantasy_weeks fw
        on m.utc_time::date between fw.week_start::date and fw.week_end::date
    join played_matches pm on m.match_id = pm.match_id

    union all

    select
        m.match_id,
        m.utc_time::date    as match_date,
        fw.week             as fantasy_week,
        m.away_team_id      as team_id
    from {{ source('fotmob', 'dim_matches') }} m
    join fantasy_weeks fw
        on m.utc_time::date between fw.week_start::date and fw.week_end::date
    join played_matches pm on m.match_id = pm.match_id
),

-- All expected player-match rows: rostered player's team played within their valid window
roster_team_matches as (
    select
        r.manager,
        r.roster_name,
        r.player_id,
        twm.match_id,
        twm.match_date,
        twm.fantasy_week
    from roster_history r
    join player_teams pt on r.player_id = pt.player_id
    join team_week_matches twm
        on pt.team_id = twm.team_id
        and twm.match_date >= r.valid_from
        and (r.valid_to is null or twm.match_date < r.valid_to)
)

select
    coalesce(mp.match_id, rtm.match_id)             as match_id,
    rtm.player_id,
    rtm.roster_name                                 as player_name,
    mp.player_name                                  as fotmob_name,
    coalesce(mp.match_date, rtm.match_date)         as match_date,
    coalesce(mp.fantasy_week, rtm.fantasy_week)     as fantasy_week,
    mp.team_name,
    mp.draft_position,
    coalesce(mp.minutes_played, 0)                  as minutes_played,
    coalesce(mp.total_points, 0)                    as total_points,
    rtm.manager,
    case
        when b.benched_player_id is not null then true
        else false
    end                                             as is_benched
from roster_team_matches rtm
left join match_points mp
    on rtm.player_id = mp.player_id
    and rtm.match_id = mp.match_id
left join benched_player_ids b
    on coalesce(mp.fantasy_week, rtm.fantasy_week) = b.week
    and rtm.manager = b.manager
    and rtm.player_id = b.benched_player_id
