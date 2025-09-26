select 
player,
squad,
season,
competition,
match_position as playing_position,
split_part(match_position, ',', 1) as primary_position,
case 
    when split_part(primary_position, ',', 1) in ('LB', 'RB') then 'Defender'
    when split_part(primary_position, ',', 1) in ('CB') then 'Defender'
    when split_part(primary_position, ',', 1) in ('LM', 'CM', 'RM', 'DM', 'AM') then 'Midfielder'
    when split_part(primary_position, ',', 1) in ('FW', 'LW', 'RW') then 'Forward'
end as position_group,
sum(minutes) as minutes,
sum(goals) as goals,
sum(assists) as assists,
sum(pk_goals) as pk_goals,
sum(pk_attempts) as pk_attempts,
sum(pk_attempts) - sum(pk_goals) as pk_misses,
sum(shots) as shots,
sum(shots_on_target) as shots_on_target,
sum(yellow_cards) as yellow_cards,
sum(red_cards) as red_cards,
sum(touches) as touches,
sum(xg) as xg,
sum(npxg) as npxg,
sum(xag) as xag,
sum(shot_creating_actions) as shot_creating_actions,
sum(goal_creating_actions) as goal_creating_actions,
sum(passes_completed) as passes_completed,
sum(passes_attempted) as passes_attempted, 
sum(total_pass_distance) as total_pass_distance,
sum(total_progressive_pass_distance) as total_progressive_pass_distance,
sum(short_passes_completed) as short_passes_completed,
sum(short_passes_attempted) as short_passes_attempted,
sum(medium_passes_completed) as medium_passes_completed,
sum(medium_passes_attempted) as medium_passes_attempted,
sum(long_passes_completed) as long_passes_completed,
sum(long_passes_attempted) as long_passes_attempted,
sum(xa) as xa,
sum(key_passes) as key_passes,
sum(passes_into_final_third) as passes_into_final_third,
sum(passes_into_penalty_area) as passes_into_penalty_area,
sum(crosses_into_penalty_area) as crosses_into_penalty_area,
sum(progressive_passes) as progressive_passes,
sum(touches_def_penalty_area) as touches_def_penalty_area,
sum(touches_def_third) as touches_def_third,
sum(touches_mid_third) as touches_mid_third,
sum(touches_att_third) as touches_att_third,
sum(touches_att_penalty_area) as touches_att_penalty_area,
sum(touches_live) as touches_live,
sum(take_ons_attempted) as take_ons_attempted,
sum(take_ons_succeeded) as take_ons_succeeded,
sum(take_ons_tackled) as take_ons_tackled,
sum(carries) as carries,
sum(total_carries_distance) as total_carries_distance,
sum(total_progressive_carries_distance) as total_progressive_carries_distance,
sum(progressive_carries) as progressive_carries,
sum(carries_into_final_third) as carries_into_final_third,
sum(carries_into_penalty_area) as carries_into_penalty_area,
sum(carries_miscontrolled) as carries_miscontrolled,
sum(carries_dispossessed) as carries_dispossessed,
sum(passes_received) as passes_received,
sum(progressive_passes_received) as progressive_passes_received,
sum(passes_live) as passes_live,
sum(passes_dead_ball) as passes_dead_ball,
sum(passes_crosses) as passes_crosses,
sum(passes_throw_ins) as passes_throw_ins,
sum(passes_switches) as passes_switches,
sum(corner_kicks_inswinging) as corner_kicks_inswinging,
sum(corner_kicks_outswinging) as corner_kicks_outswinging, 
sum(corner_kicks_straight) as corner_kicks_straight, 
sum(passes_offside) as passes_offside,
sum(passes_blocked) as passes_blocked,
sum(tackles_att) as tackles_att,
sum(tackles_won) as tackles_won,
sum(tackles_def_third) as tackles_def_third,
sum(tackles_mid_third) as tackles_mid_third,
sum(tackles_att_third) as tackles_att_third,
sum(challenges_won) as challenges_won,
sum(challenges_att) as challenges_att,
sum(blocks) as blocks,
sum(shot_blocks) as shot_blocks,
sum(pass_blocks) as pass_blocks,
sum(interceptions) as interceptions,
sum(clearances) as clearances,
sum(errors_lead_to_shot) as errors_lead_to_shot,
sum(fouled) as fouled,
sum(fouls) as fouls,
sum(second_yellow_cards) as second_yellow_cards,
sum(offsides) as offsides,
sum(pks_won) as pks_won,
sum(ball_recoveries) as ball_recoveries,
sum(aerial_duels_won) as aerial_duels_won,
sum(aerial_duels_lost) as aerial_duels_lost,
sum(own_goals) as own_goals,
sum(crosses) as crosses,
case 
    when sum(passes_attempted) > 0 then sum(passes_completed::numeric)/nullif(sum(passes_attempted), 0)
    else 0
end as pass_completion_rate,
case 
    when sum(short_passes_attempted) > 0 then sum(short_passes_completed::numeric)/nullif(sum(short_passes_attempted), 0)
    else 0
end as short_pass_completion_rate,
case 
    when sum(medium_passes_attempted) > 0 then sum(medium_passes_completed::numeric)/nullif(sum(medium_passes_attempted), 0)
    else 0
end as medium_pass_completion_rate,
case 
    when sum(long_passes_attempted) > 0 then sum(long_passes_completed::numeric)/nullif(sum(long_passes_attempted), 0)
    else 0
end as long_pass_completion_rate,
case
    when sum(challenges_att) + sum(challenges_won) = 0 then 0
    else sum(challenges_won)::numeric/sum(challenges_att)
end as challenge_success_rate,
case
    when sum(tackles_att) = 0 then 0
    else sum(tackles_won)::numeric/sum(tackles_att)
end as tackle_success_rate,
case 
    when sum(aerial_duels_won) + sum(aerial_duels_lost) = 0 then 0
    else sum(aerial_duels_won::numeric)/(sum(aerial_duels_won) + sum(aerial_duels_lost))
end as aerial_duel_win_rate,
case 
    when sum(carries) > 0 then (sum(carries) - sum(carries_miscontrolled) - sum(carries_dispossessed)) / nullif(sum(carries::numeric), 0) * 100 
    else 0
end as carry_control_rate,
case 
    when sum(npxg) > 0 then sum(goals::numeric)/sum(npxg)
    else 0
end as xg_conversion_rate,
case 
    when sum(shots) > 0 then sum(shots_on_target::numeric)/sum(shots)
    else 0
end as shot_accuracy_rate,
case 
    when sum(take_ons_attempted) > 0 then sum(take_ons_succeeded::numeric)/sum(take_ons_attempted)
    else 0
end as take_on_success_rate
from {{ ref('player_match_full_stats') }}
group by 1,2,3,4,5,6,7