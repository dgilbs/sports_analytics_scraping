CREATE TABLE "dim_matches"(
    id varchar(30) primary key,
    match_date date,
    competition_id int,
    home_team_id varchar(30),
    away_team_id varchar(30),
    referee varchar(50),
    season varchar(20),
    attendance int,
    venue varchar(40),
    home_goals int,
    away_goals int

);