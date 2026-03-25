with player_stats as (
    select * from {{ ref('player_match_stats') }}
),

fantasy_weeks as (
    select * from {{ ref('fantasy_weeks_2026') }}
),

player_positions as (
    with combined as (
        select m.player_id, d."Position" as position, 1 as priority
        from {{ ref('player_id_mapping') }} m
        inner join {{ ref('draft_list') }} d on m.seed_name = d.name
        union all
        select m.player_id, d.draft_position as position, 2 as priority
        from {{ ref('player_id_mapping_2024') }} m
        inner join {{ ref('draft_list_2024') }} d on m.seed_name = d.player
    ),
    ranked as (
        select *,
               row_number() over (partition by player_id order by priority) as rn
        from combined
    )
    select player_id, position
    from ranked
    where rn = 1
),

player_cards as (
    select
        player_id::bigint   as player_id,
        match_id::bigint    as match_id,
        coalesce(yellow_cards, 0) as yellow_cards,
        coalesce(red_cards, 0)    as red_cards
    from {{ ref('player_cards_2026') }}
),

penalty_saves as (
    select * from {{ ref('penalty_saves') }}
),

own_goals as (
    select
        player_id::bigint   as player_id,
        match_id::bigint    as match_id,
        coalesce(own_goals, 0) as own_goals
    from {{ ref('player_own_goals_2026') }}
),

pk_attempts as (
    select
        player_id::bigint                                   as player_id,
        match_id::bigint                                    as match_id,
        count(*)                                            as pk_att,
        count(*) filter (where succeeded::boolean)          as pk_made
    from {{ ref('pk_attempts') }}
    group by 1, 2
),

roster_names as (
    select distinct on (player_id::bigint)
        player_id::bigint as player_id,
        player            as roster_name
    from {{ ref('nwsfl_roster_matched') }}
    order by player_id::bigint
),

draft_names as (
    select distinct on (player_id::bigint)
        player_id::bigint as player_id,
        seed_name         as draft_name
    from {{ ref('player_id_mapping') }}
    order by player_id::bigint
),

base as (
    select
        ps.*,
        fw.week                             as fantasy_week,
        pp.position                         as draft_position,
        coalesce(pc.yellow_cards, 0)        as yellow_cards,
        coalesce(pc.red_cards, 0)           as red_cards,
        coalesce(pks.pk_saves, 0)           as pk_saves,
        coalesce(og.own_goals, 0)           as own_goals,
        coalesce(pka.pk_made, 0)            as pk_made,
        coalesce(pka.pk_att, 0)             as pk_att,
        coalesce(rn.roster_name, dn.draft_name, ps.player_name) as display_name
    from player_stats ps
    inner join fantasy_weeks fw
        on ps.match_date between fw.week_start::date and fw.week_end::date
    left join player_positions pp
        on ps.player_id = pp.player_id
    left join player_cards pc
        on ps.player_id = pc.player_id
        and ps.match_id = pc.match_id
    left join penalty_saves pks
        on ps.player_id = pks.player_id
        and ps.match_id = pks.match_id
    left join own_goals og
        on ps.player_id = og.player_id
        and ps.match_id = og.match_id
    left join pk_attempts pka
        on ps.player_id = pka.player_id
        and ps.match_id = pka.match_id
    left join roster_names rn
        on ps.player_id = rn.player_id
    left join draft_names dn
        on ps.player_id = dn.player_id
)

select
    fantasy_week                                                                as week,
    player_id,
    display_name                                                                as player,
    round(avg(fotmob_rating)::numeric, 1)                                       as fotmob_rating,
    team_name,
    draft_position                                                              as pos,
    max(shirt_number)                                                           as number,
    sum(minutes_played)                                                         as min_played,
    sum(coalesce(goals, 0))                                                     as gls,
    sum(coalesce(assists, 0))                                                   as ast,
    sum(pk_made)                                                                as pk_made,
    sum(pk_att)                                                                 as pk_att,
    null::integer                                                               as sh,          -- total shots not in source
    sum(coalesce(shots_on_target, 0))                                           as sot,
    sum(yellow_cards)                                                           as crdy,
    sum(red_cards)                                                              as crdr,
    sum(coalesce(touches, 0))                                                   as touches,
    sum(coalesce(tackles, 0))                                                   as tkl,
    sum(coalesce(interceptions, 0))                                             as int,
    sum(coalesce(blocks, 0))                                                    as blocks,
    round(sum(coalesce(xg, 0))::numeric, 2)                                     as xg,
    null::numeric                                                               as npxg,        -- not in source
    round(sum(coalesce(xa, 0))::numeric, 2)                                     as xag,
    null::integer                                                               as sca,         -- not in source
    sum(coalesce(chances_created, 0))                                           as gca,
    sum(coalesce(accurate_passes_succeeded, 0))                                 as pass_cmp,
    sum(coalesce(accurate_passes_attempted, 0))                                 as pass_att,
    case
        when sum(coalesce(accurate_passes_attempted, 0)) > 0
        then round((sum(coalesce(accurate_passes_succeeded, 0))::numeric
                  / sum(coalesce(accurate_passes_attempted, 0)) * 100), 1)
        else null
    end                                                                         as pass_cmp_pct,
    sum(coalesce(passes_into_final_third, 0))                                   as prg_pass,
    null::integer                                                               as carries,     -- not in source
    null::integer                                                               as prg_carries, -- not in source
    sum(coalesce(successful_dribbles_attempted, 0))                             as carry_att,
    sum(coalesce(successful_dribbles_succeeded, 0))                             as carry_succ,
    sum(own_goals)                                                              as og,
    sum(coalesce(tackles, 0))                                                   as tkl_won,
    sum(coalesce(saves, 0))                                                     as saves,
    sum(pk_saves)                                                               as pk_saves,
    sum(coalesce(goals_conceded, 0))                                            as goals_conc
from base
group by fantasy_week, player_id, display_name, team_name, draft_position
order by fantasy_week, team_name, display_name
