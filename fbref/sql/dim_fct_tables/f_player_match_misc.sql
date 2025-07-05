create table if not exists soccer.f_player_match_misc(
    id varchar(50) primary key,
    player_id varchar(20),
    team_id varchar(20),
    match_id varchar(20),
    minutes int,
    yellow_cards int,
    red_cards int,
    second_yellow_cards int,
    fouls int,
    fouled int,
    offsides int,
    crosses int,
    pks_won int,
    ball_recoveries int,
    aerial_duels_won int,
    aerial_duels_lost int,
    own_goals int
)