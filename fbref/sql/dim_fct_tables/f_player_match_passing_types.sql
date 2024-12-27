-- f_player_match_passing_types definition

CREATE TABLE f_player_match_passing_types(
    id varchar(40) primary key,
    player_id varchar(20),
    team_id varchar(20),
    match_id varchar(20),
    minutes int,
    passes_attempted int,
    passes_live int,
    passes_dead_ball int,
    passes_crosses int,
    passes_throw_ins int,
    passes_switches int,
    corner_kicks_inswinging int,
    corner_kicks_outswinging int, 
    corner_kicks_straight int, 
    passes_completed int,
    passes_offside int,
    passes_blocked int
);