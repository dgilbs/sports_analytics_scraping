{{
    config(
        materialized="view",
    )
}}

SELECT
  dp.player,
  ds.squad,
  dsa.squad AS opponent,
  dtm.season,
  dtm.match_date,
  dc.competition,
  dsr.playing_position AS roster_position,
  dpa.position AS match_position,
  fpms.minutes,
  fpms.yellow_cards,
  fpms.red_cards,
  fpms.second_yellow_cards,
  fpms.fouls,
  fpms.fouled,
  fpms.offsides,
  fpms.crosses,
  fpms.pks_won,
  fpms.ball_recoveries,
  fpms.aerial_duels_won,
  fpms.aerial_duels_lost,
  fpms.own_goals,
  case
    when aerial_duels_won + aerial_duels_lost = 0 then 0 
    else aerial_duels_won::numeric/(aerial_duels_won + aerial_duels_lost)
  end as aerial_duel_win_rate,
  split_part(dpa.position, ',', 1) AS primary_position,
  case 
    when split_part(dpa."position", ',', 1) in ('LB', 'RB') then 'Defender'
    when split_part(dpa."position", ',', 1) in ('CB') then 'Defender'
    when split_part(dpa."position", ',', 1) in ('LM', 'CM', 'RM', 'DM', 'AM') then 'Midfielder'
    when split_part(dpa."position", ',', 1) in ('FW', 'LW', 'RW') then 'Forward'
  end as position_group,
  coalesce(
    split_part(dpa.position, ',', 1) IN ('FW', 'RW', 'LW'),
    FALSE
  ) AS is_forward,
  coalesce(split_part(dpa.position, ',', 1) IN (
    'AM', 'CM', 'RM', 'LM', 'DM', 'MF'
  ),
  FALSE) AS is_midfielder,
  coalesce(
    split_part(dpa.position, ',', 1) IN ('CB', 'WB', 'RB', 'LB'),
    FALSE
  ) AS is_defender,
  coalesce(split_part(dpa.position, ',', 1) = 'GK', FALSE) AS is_goalkeeper,
  coalesce(
    split_part(dpa.position, ',', 1) IN ('RW', 'LW', 'WB', 'RB', 'LB'),
    FALSE
  ) AS is_winger
FROM soccer.f_player_match_misc AS fpms
LEFT JOIN soccer.dim_players AS dp
  ON fpms.player_id = dp.id
LEFT JOIN soccer.dim_squads AS ds
  ON fpms.team_id = ds.id
LEFT JOIN soccer.dim_team_matches AS dtm
  ON fpms.match_id = dtm.match_id AND fpms.team_id = dtm.team_id
LEFT JOIN soccer.dim_squads AS dsa
  ON dtm.opponent_id = dsa.id
LEFT JOIN soccer.dim_competitions AS dc
  ON dtm.competition_id = dc.id
LEFT JOIN soccer.dim_player_appearances AS dpa
  ON fpms.id = dpa.id
LEFT JOIN soccer.dim_squad_rosters AS dsr
  ON
    fpms.player_id = dsr.player_id
    AND fpms.team_id = dsr.squad_id
    AND cast(dsr.season AS text) = dtm.season
