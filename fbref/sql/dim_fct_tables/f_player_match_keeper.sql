-- f_player_match_keeper definition

CREATE TABLE f_player_match_keeper(
	id varchar(50) PRIMARY KEY, 
	player_id varchar(20),
	team_id varchar(20),
	match_id varchar(20),
	minutes int,
	shots_on_target_against int,
	goals_allowed int,
	saves int,
	psxg float,
	passes_launched_completed int,
	passes_launched_attempted int,
	passes_attempted_total int,
	throws_attempted int,
	average_pass_length float,
	goal_kicks_attempted int,
	pct_goal_kicks_launched float,
	pct_of_passes_launched float,
	avg_goal_kick_length float,
	crosses_faced int,
	crosses_stopped int,
	defensive_actions_outside_pen_area int,
	avg_distance_of_defensive_actions float
);