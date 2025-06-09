CREATE TABLE if not exists soccer.dim_player_appearances(

    id varchar(50) primary key,
    player_id varchar(30),
    team_id varchar(30),
    match_id varchar(30),
    shirtnumber int,
    position varchar(20),
    age varchar(10), 
    minutes int

);
