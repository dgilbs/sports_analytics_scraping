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
sum(yellow_cards) as yellow_cards,
sum(red_cards) as red_cards,
sum(second_yellow_cards) as second_yellow_cards,
sum(fouls) as fouls,
sum(fouled) as fouled,
sum(offsides) as offsides,
sum(crosses) as crosses,
sum(pks_won) as pks_won,
sum(ball_recoveries) as ball_recoveries,
sum(aerial_duels_won) as aerial_duels_won,
sum(aerial_duels_lost) as aerial_duels_lost,
sum(own_goals) as own_goals,
case 
    when sum(aerial_duels_won) + sum(aerial_duels_lost) = 0 then 0
    else sum(aerial_duels_won::numeric)/sum(aerial_duels_lost)
end as aerial_duel_win_rate
from {{ref('player_match_misc')}}
group by 1,2,3,4,5,6,7