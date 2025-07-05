create table if not exists soccer.dim_squad_rosters(
    id varchar(50) primary key,
    player_id varchar(20),
    squad_id varchar(20),
    season varchar(20),
    pos varchar(20),
    age varchar(20),
    nation varchar(20)
);