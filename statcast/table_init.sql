CREATE SCHEMA IF NOT EXISTS statcast;

DROP TABLE IF EXISTS statcast.pitch_events;

CREATE TABLE IF NOT EXISTS statcast.pitch_events (
	pitch_type VARCHAR(2),
	game_date DATE,
	release_speed NUMERIC(4,1),
	release_pos_x NUMERIC(6,4),
	release_pos_z NUMERIC(6,4),
	player_name VARCHAR(50),
	batter_id INTEGER,
	pitcher_id INTEGER,
	event VARCHAR(50),
	description VARCHAR(20),
	location_zone INTEGER,
	game_type VARCHAR(2),
	batter_hand VARCHAR(2),
	pitcher_hand VARCHAR(2),
	home_team VARCHAR(3),
	away_team VARCHAR(3),
	_type VARCHAR(5),
	hit_location INTEGER,
	batted_ball_type VARCHAR(15),
	count_balls INTEGER,
	count_strikes INTEGER,
	game_year INTEGER,
	pfx_x NUMERIC(6,4),
	pfx_z NUMERIC(6,4),
	plate_x NUMERIC(6,4),
	plate_z NUMERIC(6,4),
	on_3b INTEGER,
	on_2B INTEGER,
	on_1B INTEGER,
	outs_when_up INTEGER,
	inning INTEGER,
	inning_half VARCHAR(3),
	hc_x NUMERIC(5,2),
	hc_y NUMERIC(5,2),
	umpire VARCHAR(50),
	sv_id VARCHAR(50),
	vx0 NUMERIC(7,4),
	vy0 NUMERIC(7,4),
	vz0 NUMERIC(7,4),
	ax NUMERIC(7,4),
	ay NUMERIC(7,4),
	az NUMERIC(7,4),
	sz_top NUMERIC(7,4),
	sz_bot NUMERIC(7,4),
	hit_distance_sc INTEGER,
	launch_speed NUMERIC(4,1),
	launch_angle NUMERIC(7,4),
	effective_speed NUMERIC(7,4),
	release_spin_rate INTEGER,
	release_extension NUMERIC(6,4),
	game_pk INTEGER,
	pos1_person_id INTEGER,
	pos2_person_id INTEGER,
	pos3_person_id INTEGER,	
	pos4_person_id INTEGER,
	pos5_person_id INTEGER,
	pos6_person_id INTEGER,
	pos7_person_id INTEGER,
	pos8_person_id INTEGER,
	pos9_person_id INTEGER,
	release_pos_y NUMERIC(7,4),
	estimated_ba_using_speedangle NUMERIC(5,4),
	estimate_woba_using_speedangle NUMERIC(5,4),
	woba_value NUMERIC(3,2),
	woba_denom NUMERIC(3,2),
	babip_value INTEGER,
	iso_value INTEGER,
	launch_speed_angle INTEGER,
	at_bat_number INTEGER,
	pitch_number INTEGER);
