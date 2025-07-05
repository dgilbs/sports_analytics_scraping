create table if not exists soccer.dim_team_matches(
    id varchar(50) primary key,
    team_id varchar(20),
    opponent_id varchar(20),
    season varchar(20),
    competition_id int,
    goals_scored int,
    goals_against int,
    xg_for float,
    xg_against float,
    match_id varchar(20),
    match_date date,
    home_or_away varchar(10)
    
)