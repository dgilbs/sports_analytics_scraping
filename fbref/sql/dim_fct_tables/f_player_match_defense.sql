CREATE TABLE f_player_match_defense(
    id varchar(40) primary key,
    player_id varchar(20),
    team_id varchar(20),
    match_id varchar(20),
    minutes int,
    tackles_att int,
    tackles_won int,
    tackles_def_third int,
    tackles_mid_third int,
    tackles_att_third int,
    challenges_won int,
    challenges_att int,
    blocks int,
    shot_blocks int,
    pass_blocks int,
    interceptions int,
    clearances int,
    errors_lead_to_shot int
);