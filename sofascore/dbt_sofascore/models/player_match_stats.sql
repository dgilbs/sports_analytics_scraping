with base as (
    select * from {{ source('sofascore', 'fact_player_match_stats') }}
),

matches as (
    select * from {{ source('sofascore', 'dim_matches') }}
)

select
    b.event_id,
    b.season,
    b.match_date,
    b.home_team,
    b.away_team,
    b.team,
    b.side,
    b.player_id,
    b.player_name,
    b.position,
    b.substitute,
    b.minutes_played,
    b.rating,
    b.rating_alternative,

    -- Match result from this player's team's perspective
    case
        when b.side = 'home' and m.winner_code = 1 then 'win'
        when b.side = 'away' and m.winner_code = 2 then 'win'
        when m.winner_code = 3                      then 'draw'
        else 'loss'
    end as result,
    m.home_score,
    m.away_score,
    case
        when b.side = 'home' then m.away_score
        else m.home_score
    end as goals_conceded,

    -- Goals & Attacking
    b.goals,
    b.assists,
    b.key_passes,
    b.total_shots,
    b.shots_on_target,
    b.shots_off_target,
    b.shots_blocked,
    b.big_chance_missed,
    b.total_offside,
    {{ safe_divide_round('b.shots_on_target', 'b.total_shots') }}     as shot_accuracy,
    {{ safe_divide_round('b.goals', 'b.total_shots') }}               as shot_conversion,

    -- Passing
    b.total_pass,
    b.accurate_pass,
    b.total_long_balls,
    b.accurate_long_balls,
    b.total_cross,
    b.accurate_cross,
    b.own_half_passes,
    b.accurate_own_half_passes,
    b.opp_half_passes,
    b.accurate_opp_half_passes,
    {{ safe_divide_round('b.accurate_pass', 'b.total_pass') }}                         as pass_completion,
    {{ safe_divide_round('b.accurate_long_balls', 'b.total_long_balls') }}             as long_ball_completion,
    {{ safe_divide_round('b.accurate_cross', 'b.total_cross') }}                       as cross_completion,
    {{ safe_divide_round('b.accurate_opp_half_passes', 'b.opp_half_passes') }}         as opp_half_pass_completion,

    -- Carries & Progression
    b.carries_count,
    b.carries_distance,
    b.progressive_carries_count,
    b.progressive_carries_distance,
    b.total_progression,
    b.best_carry_progression,
    {{ safe_divide_round('b.progressive_carries_count', 'b.carries_count') }}          as progressive_carry_rate,

    -- Touches & Possession
    b.touches,
    b.unsuccessful_touch,
    b.possession_lost,
    b.dispossessed,

    -- Duels
    b.duel_won,
    b.duel_lost,
    b.aerial_won,
    b.aerial_lost,
    b.total_contest,
    b.won_contest,
    b.challenge_lost,
    {{ safe_divide_round('b.duel_won', '(b.duel_won + b.duel_lost)') }}               as duel_win_rate,
    {{ safe_divide_round('b.aerial_won', '(b.aerial_won + b.aerial_lost)') }}         as aerial_win_rate,
    {{ safe_divide_round('b.won_contest', 'b.total_contest') }}                       as contest_win_rate,

    -- Defending
    b.total_tackle,
    b.won_tackle,
    b.interception_won,
    b.total_clearance,
    b.ball_recovery,
    {{ safe_divide_round('b.won_tackle', 'b.total_tackle') }}                         as tackle_success_rate,

    -- Discipline
    b.fouls,
    b.was_fouled,

    -- Value metrics
    b.shot_value,
    b.pass_value,
    b.dribble_value,
    b.defensive_value

from base b
left join matches m on b.event_id = m.event_id
where b.minutes_played is not null
