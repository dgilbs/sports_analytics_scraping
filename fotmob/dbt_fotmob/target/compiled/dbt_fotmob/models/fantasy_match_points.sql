with players as (
    select * from "neondb"."fotmob"."player_match_stats"
),

-- Map draft positions to player_ids, preferring 2025 season when player
-- appears in both seasons (e.g. same player changed position year-over-year)
player_positions as (
    with combined as (
        select m.player_id, d."Position" as position, 1 as priority
        from "neondb"."fotmob"."player_id_mapping" m
        inner join "neondb"."fotmob"."draft_list" d on m.seed_name = d.name
        union all
        select m.player_id, d.draft_position as position, 2 as priority
        from "neondb"."fotmob"."player_id_mapping_2024" m
        inner join "neondb"."fotmob"."draft_list_2024" d on m.seed_name = d.player
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

-- Card data from seed file
player_cards as (
    select
        player_id::bigint                                   as player_id,
        match_id::bigint                                    as match_id,
        coalesce(yellow_cards, 0)                           as yellow_cards,
        coalesce(red_cards, 0)                              as red_cards
    from "neondb"."fotmob"."player_cards_2026"
),

-- Penalty saves seed
penalty_saves as (
    select * from "neondb"."fotmob"."penalty_saves"
),

-- Fantasy week lookup
fantasy_weeks as (
    select * from "neondb"."fotmob"."fantasy_weeks_2026"
),

-- Opponent per team per match
opponents as (
    select
        m.match_id,
        m.home_team_id                          as team_id,
        t.team_name                             as opponent_name
    from "neondb"."fotmob"."dim_matches" m
    join "neondb"."fotmob"."dim_teams" t on m.away_team_id = t.team_id
    union all
    select
        m.match_id,
        m.away_team_id                          as team_id,
        t.team_name                             as opponent_name
    from "neondb"."fotmob"."dim_matches" m
    join "neondb"."fotmob"."dim_teams" t on m.home_team_id = t.team_id
),

-- Goals scored per team per match (sum of all player goals)
team_scored as (
    select
        match_id,
        team_id,
        sum(coalesce(goals, 0)) as goals_scored
    from players
    where team_id is not null
    group by 1, 2
),

-- Goals conceded per team = sum of GK goals_conceded stat (more reliable than
-- summing opponent goals, handles own goals and tracking gaps correctly)
team_conceded as (
    select
        match_id,
        team_id,
        sum(coalesce(goals_conceded, 0)) as goals_conceded
    from players
    where goals_conceded is not null
      and team_id is not null
    group by 1, 2
),

base as (
    select
        p.*,
        fw.week                                             as fantasy_week,
        pp.position                                         as draft_position,
        coalesce(tc.goals_conceded, 0)                      as team_goals_conceded,
        coalesce(ts.goals_scored, 0)                        as team_goals_scored,
        case when coalesce(tc.goals_conceded, 0) = 0
             then true else false end                       as clean_sheet,
        coalesce(pc.yellow_cards, 0)                        as yellow_cards,
        coalesce(pc.red_cards, 0)                           as red_cards,
        0                                                   as own_goals,
        0                                                   as pks_won,
        0                                                   as pks_missed,
        coalesce(ps.pk_saves, 0)                            as pk_saves,
        o.opponent_name
    from players p
    left join player_positions pp on p.player_id = pp.player_id
    left join team_conceded tc
        on p.match_id = tc.match_id
        and p.team_id = tc.team_id
    left join team_scored ts
        on p.match_id = ts.match_id
        and p.team_id = ts.team_id
    left join player_cards pc
        on p.player_id = pc.player_id
        and p.match_id = pc.match_id
    left join penalty_saves ps
        on p.player_id = ps.player_id
        and p.match_id = ps.match_id
    left join opponents o
        on p.match_id = o.match_id
        and p.team_id = o.team_id
    left join fantasy_weeks fw
        on p.match_date between fw.week_start::date and fw.week_end::date
)

select
    match_id,
    player_id,
    player_name,
    match_date,
    fantasy_week,
    season,
    team_id,
    team_name,
    draft_position,
    opponent_name,
    minutes_played,
    clean_sheet,
    team_goals_conceded,
    team_goals_scored,

    -- ── ALL POSITIONS ────────────────────────────────────────────────────────

    case when coalesce(minutes_played, 0) > 0  then 1  else 0 end               as pts_appearance,
    case when coalesce(minutes_played, 0) > 60 then 2  else 0 end               as pts_60_minutes,
    coalesce(assists, 0) * 2                                                     as pts_assists,
    coalesce(interceptions, 0) * 0.5                                             as pts_interceptions,
    coalesce(blocks, 0) * 0.5                                                    as pts_blocks,
    0                                                                            as pts_goal_creating_actions,
    coalesce(successful_dribbles_succeeded, 0) * 0.5                             as pts_successful_takeons,
    case when coalesce(touches, 0) > 60 then 2 else 0 end                        as pts_touches,
    case when coalesce(accurate_passes_pct, 0) > 0.85
          and coalesce(accurate_passes_attempted, 0) >= 20 then 2 else 0 end     as pts_pass_completion,

    -- From FBRef via crossref (null if player not in crossref)
    greatest(yellow_cards * -2, -4)                                              as pts_yellow_cards,
    red_cards * -6                                                               as pts_red_card,
    pks_won * 2                                                                  as pts_penalty_converted,
    own_goals * -3                                                               as pts_own_goal,
    pks_missed * -3                                                              as pts_penalty_missed,

    -- ── POSITION-SPECIFIC ────────────────────────────────────────────────────

    -- Goals (FW=4, MF=5, DF=6, GK=10)
    case draft_position
        when 'FW' then coalesce(goals, 0) * 4
        when 'MF' then coalesce(goals, 0) * 5
        when 'DF' then coalesce(goals, 0) * 6
        when 'GK' then coalesce(goals, 0) * 10
        else 0
    end                                                                          as pts_goals,

    -- Tackles Won (FW=1pt, MF=0.5pt, DF=0.5pt, GK=none)
    case draft_position
        when 'FW' then coalesce(tackles, 0) * 1.0
        when 'MF' then coalesce(tackles, 0) * 0.5
        when 'DF' then coalesce(tackles, 0) * 0.5
        else 0
    end                                                                          as pts_tackles,

    -- Clean Sheet (GK=5, DF=4 if 60+min, MF=2)
    case draft_position
        when 'GK' then case when clean_sheet then 5 else 0 end
        when 'DF' then case when clean_sheet
                             and coalesce(minutes_played, 0) >= 60 then 4 else 0 end
        when 'MF' then case when clean_sheet then 2 else 0 end
        else 0
    end                                                                          as pts_clean_sheet,

    -- Goals Conceded (DF=-0.5 each, GK=-1 each)
    case draft_position
        when 'DF' then team_goals_conceded * -0.5
        when 'GK' then team_goals_conceded * -1.0
        else 0
    end                                                                          as pts_goals_conceded,

    -- GK only
    case when draft_position = 'GK'
         then coalesce(saves, 0) * 0.5 else 0 end                               as pts_saves,
    case when draft_position = 'GK'
         then pk_saves * 5 else 0 end                                            as pts_penalty_save,

    -- ── RAW STATS (for aggregation upstream) ─────────────────────────────────
    coalesce(goals, 0)                                                           as goals,
    coalesce(assists, 0)                                                         as assists,
    coalesce(tackles, 0)                                                         as tackles_won,

    -- ── TOTAL ────────────────────────────────────────────────────────────────
    round(cast(
        case when coalesce(minutes_played, 0) > 0  then 1  else 0 end
        + case when coalesce(minutes_played, 0) > 60 then 2 else 0 end
        + coalesce(assists, 0) * 2
        + coalesce(interceptions, 0) * 0.5
        + coalesce(blocks, 0) * 0.5

        + coalesce(successful_dribbles_succeeded, 0) * 0.5
        + case when coalesce(touches, 0) > 60 then 2 else 0 end
        + case when coalesce(accurate_passes_pct, 0) > 0.85
               and coalesce(accurate_passes_attempted, 0) >= 20 then 2 else 0 end
        + greatest(yellow_cards * -2, -4)
        + red_cards * -6
        + pks_won * 2
        + own_goals * -3
        + pks_missed * -3
        + case draft_position
            when 'FW' then coalesce(goals, 0) * 4
            when 'MF' then coalesce(goals, 0) * 5
            when 'DF' then coalesce(goals, 0) * 6
            when 'GK' then coalesce(goals, 0) * 10
            else 0 end
        + case draft_position
            when 'FW' then coalesce(tackles, 0) * 1.0
            when 'MF' then coalesce(tackles, 0) * 0.5
            when 'DF' then coalesce(tackles, 0) * 0.5
            else 0 end
        + case draft_position
            when 'GK' then case when clean_sheet then 5 else 0 end
            when 'DF' then case when clean_sheet
                               and coalesce(minutes_played, 0) >= 60 then 4 else 0 end
            when 'MF' then case when clean_sheet then 2 else 0 end
            else 0 end
        + case draft_position
            when 'DF' then team_goals_conceded * -0.5
            when 'GK' then team_goals_conceded * -1.0
            else 0 end
        + case when draft_position = 'GK' then coalesce(saves, 0) * 0.5 else 0 end
        + case when draft_position = 'GK' then pk_saves * 5 else 0 end
    as numeric), 1)                                                              as total_points

from base