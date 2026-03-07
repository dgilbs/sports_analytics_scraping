with stats as (
    select * from "neondb"."fotmob"."fact_player_stats"
),
lineups as (
    select * from "neondb"."fotmob"."fact_lineups"
),
players as (
    select * from "neondb"."fotmob"."dim_players"
),
matches as (
    select * from "neondb"."fotmob"."dim_matches"
),
teams as (
    select * from "neondb"."fotmob"."dim_teams"
)
select
    s.match_id,
    s.player_id,
    p.player_name,
    m.utc_time::date         as match_date,
    m.season,
    l.team_id,
    t.team_name,
    l.side,                  -- 'home' | 'away'
    l.bucket,                -- 'starters' | 'bench'
    l.formation,
    l.position_id,
    l.usual_position_id,
    l.shirt_number,
    case
        when l.position_id = 11                    then 'Goalkeeper'
        when l.position_id between 30 and 39       then 'Defender'
        when l.position_id between 70 and 79       then 'Midfielder'
        when l.position_id between 100 and 109     then 'Forward'
        else 'Unknown'
    end as position_group,
    -- performance
    s.fotmob_rating,
    s.minutes_played,
    s.goals,
    s.assists,
    s.xg,
    s.xa,
    s.xg_plus_xa,
    s.shots_on_target,
    s.touches,
    s.touches_in_opposition_box,
    s.chances_created,
    s.offsides,
    s.dispossessed,
    -- passing
    s.passes_into_final_third,
    s.accurate_passes_succeeded,
    s.accurate_passes_attempted,
    s.accurate_passes_pct,
    s.accurate_crosses_succeeded,
    s.accurate_crosses_attempted,
    s.accurate_crosses_pct,
    s.accurate_long_balls_succeeded,
    s.accurate_long_balls_attempted,
    s.accurate_long_balls_pct,
    -- dribbles
    s.successful_dribbles_succeeded,
    s.successful_dribbles_attempted,
    s.successful_dribbles_pct,
    -- defense
    s.tackles,
    s.interceptions,
    s.blocks,
    s.clearances,
    s.headed_clearance,
    s.recoveries,
    s.dribbled_past,
    s.defensive_contributions,
    -- duels
    s.duels_won,
    s.duels_lost,
    s.ground_duels_won_succeeded,
    s.ground_duels_won_attempted,
    s.ground_duels_won_pct,
    s.aerial_duels_won_succeeded,
    s.aerial_duels_won_attempted,
    s.aerial_duels_won_pct,
    -- discipline
    s.fouls_committed,
    s.was_fouled,
    -- goalkeeper
    s.saves,
    s.goals_conceded,
    s.xgot,
    s.xgot_faced,
    s.goals_prevented,
    s.acted_as_sweeper,
    s.high_claim
from stats s
left join lineups l  on s.match_id = l.match_id and s.player_id = l.player_id
left join players p  on s.player_id = p.player_id
left join matches m  on s.match_id  = m.match_id
left join teams   t  on l.team_id   = t.team_id