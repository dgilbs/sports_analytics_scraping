{{
    config(
        materialized="view",
    )
}}


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
sum(touches) as touches,
sum(touches_def_penalty_area) as touches_def_penalty_area,
sum(touches_def_third) as touches_def_third,
sum(touches_mid_third) as touches_mid_third,
sum(touches_att_third) as touches_att_third,
sum(touches_att_penalty_area) as touches_att_penalty_area,
sum(touches_live) as touches_live,
sum(take_ons_attempted) as take_ons_attempted,
sum(take_ons_succeeded) as take_ons_succeeded,
sum(take_ons_tackled)take_ons_tackled,
sum(carries) carries,
sum(total_carries_distance) total_carries_distance,
sum(total_progressive_carries_distance) as total_progressive_carries_distance,
sum(progressive_carries) as progressive_carries,
sum(carries_into_final_third) as carries_into_final_third,
sum(carries_into_penalty_area) as carries_into_penalty_area,
sum(carries_miscontrolled) as carries_miscontrolled,
sum(carries_dispossessed) as carries_dispossessed,
sum(passes_recieved) as passes_recieved,
sum(progressive_passes_recieved) as progressive_passes_recieved,
round((sum(touches) * 1.0/sum(minutes)) * 90.0,3) as touches_per_90,
round((sum(touches_def_penalty_area) * 1.0/sum(minutes)) * 90.0,3) as touches_def_penalty_area_per_90,
round((sum(touches_def_third) * 1.0/sum(minutes)) * 90.0,3) as touches_def_third_per_90,
round((sum(touches_mid_third) * 1.0/sum(minutes)) * 90.0,3) as touches_mid_third_per_90,
round((sum(touches_att_third) * 1.0/sum(minutes)) * 90.0,3) as touches_att_third_per_90,
round((sum(touches_att_penalty_area) * 1.0/sum(minutes)) * 90.0,3) as touches_att_penalty_area_per_90,
round((sum(touches_live) * 1.0/sum(minutes)) * 90.0,3) as touches_live_per_90,
round((sum(take_ons_attempted) * 1.0/sum(minutes)) * 90.0,3) as take_ons_attempted_per_90,
round((sum(take_ons_succeeded) * 1.0/sum(minutes)) * 90.0,3) as take_ons_succeeded_per_90,
round((sum(take_ons_tackled) * 1.0/sum(minutes)) * 90.0,3) as take_ons_tackled_per_90,
round((sum(carries) * 1.0/sum(minutes)) * 90.0,3) as carries_per_90,
round((sum(total_carries_distance) * 1.0/sum(minutes)) * 90.0,3) as total_carries_distance_per_90,
round((sum(total_progressive_carries_distance) * 1.0/sum(minutes)) * 90.0,3) as total_progressive_carries_distance_per_90,
round((sum(progressive_carries) * 1.0/sum(minutes)) * 90.0,3) as progressive_carries_per_90,
round((sum(carries_into_final_third) * 1.0/sum(minutes)) * 90.0,3) as carries_into_final_third_per_90,
round((sum(carries_into_penalty_area) * 1.0/sum(minutes)) * 90.0,3) as carries_into_penalty_area_per_90,
round((sum(carries_miscontrolled) * 1.0/sum(minutes)) * 90.0,3) as carries_miscontrolled_per_90,
round((sum(carries_dispossessed) * 1.0/sum(minutes)) * 90.0,3) as carries_dispossessed_per_90,
round((sum(passes_recieved) * 1.0/sum(minutes)) * 90.0,3) as passes_recieved_per_90,
round((sum(progressive_passes_recieved) * 1.0/sum(minutes)) * 90.0,3) as progressive_passes_recieved_per_90,
(sum(carries) - sum(carries_miscontrolled) - sum(carries_dispossessed)) / nullif(sum(carries), 0) * 100 as carry_control_rate,
sum(take_ons_succeeded)/nullif(sum(take_ons_attempted), 0) as take_on_success_rate
from {{ref('player_match_possession')}}
group by 1,2,3,4,5,6,7