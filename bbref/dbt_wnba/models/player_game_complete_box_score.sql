{{
    config(
        materialized="view",
    )
}}


select 
bas.player,
bas.team,
bas.team AS opponent,
bas.game_id,
bas.is_playoffs,
bas.is_commissioners_cup,
bas.game_date,
bas.season,
bas.playing_position,
bas.is_center,
bas.is_forward,
bas.is_guard,
bas.is_perimeter,
bas.is_post,
bas.game_result,
bas.field_goals,
bas.field_goal_attempts,
bas.field_goals_threes,
bas.field_goal_threes_attempts,
bas.free_throws,
bas.free_throw_attempts,
bas.offensive_rebounds,
bas.total_rebounds,
bas.assists, 
bas.steals,
bas.blocks,
bas.turnovers,
bas.personal_fouls,
bas.points, 
bas.plus_minus,
bas.minutes_played,
adv.true_shooting_pct,
adv.effective_field_goal_pct,
adv.three_point_attempt_rate,
adv.defensive_rebound_pct,
adv.offensive_rebound_pct,
adv.assist_pct,
adv.steal_pct,
adv.usage_rate,
adv.offensive_rating,
adv.defensive_rating
from {{ ref('player_game_box_scores') }} bas
left join {{ ref('player_game_advanced_box_scores') }} adv 
on adv.player_id = bas.player_id and bas.game_id = adv.game_id