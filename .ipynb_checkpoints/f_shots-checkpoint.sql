create table if not exists soccer.f_shots(
    id varchar(50) primary key,
    xg float,
    psxg float,
    outcome varchar(20),
    distance int, 
    body_part varchar(20),
    notes text,
    player_id varchar(20),
    team_id varchar(20),
    match_id varchar(20),
    sca_1_player_id varchar(20),
    sca_2_player_id varchar(20)  
);