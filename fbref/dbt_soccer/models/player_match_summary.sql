
{{
    config(
        materialized="view",
    )
}}


select 
dp.player,
ds.squad,
dsa.squad as opponent,
dtm.season,
dtm.match_date,
dc.competition,
dsr.playing_position as roster_position,
dpa."position" as match_postion,
fpms.minutes,
goals,
assists,
pk_goals,
pk_attempts,
pk_attempts - pk_goals as pk_misses,
shots,
shots_on_target,
yellow_cards,
red_cards,
touches,
tackles,
interceptions,
blocks,
xg,
npxg,
xag,
shot_creating_actions,
goal_creating_actions
from soccer.f_player_match_summary fpms
left join soccer.dim_players dp 
on dp.id = fpms.player_id
left join soccer.dim_squads ds 
on ds.id = fpms.team_id
left join soccer.dim_team_matches dtm
on dtm.match_id = fpms.match_id and dtm.team_id = fpms.team_id
left join soccer.dim_squads dsa 
on dsa.id = dtm.opponent_id
left join soccer.dim_competitions dc
on dc.id = dtm.competition_id
left join soccer.dim_player_appearances dpa 
on dpa.id = fpms.id
left join soccer.dim_squad_rosters dsr 
on dsr.player_id = fpms.player_id and fpms.team_id = dsr.squad_id and cast(dsr.season as text) = dtm.season