import duckdb

DDL = r'''
CREATE TABLE IF NOT EXISTS teams (
  team_id VARCHAR, team_name VARCHAR, league VARCHAR, season VARCHAR
);

CREATE TABLE IF NOT EXISTS players (
  player_id VARCHAR, player_name VARCHAR, dob DATE, nationality VARCHAR,
  primary_pos VARCHAR, foot VARCHAR, height_cm INT
);

CREATE TABLE IF NOT EXISTS player_season_stats (
  player_id VARCHAR, team_id VARCHAR, league VARCHAR, season VARCHAR,
  minutes INT,
  passes_completed_per90 DOUBLE, passes_attempted_per90 DOUBLE,
  progressive_passes_per90 DOUBLE, progressive_carries_per90 DOUBLE,
  carries_into_final_third_per90 DOUBLE,
  dribbles_completed_per90 DOUBLE,
  shots_per90 DOUBLE, xg_per90 DOUBLE, xa_per90 DOUBLE,
  tackles_interceptions_per90 DOUBLE,
  pressures_per90 DOUBLE, press_success_pct DOUBLE,
  aerials_won_pct DOUBLE,
  crosses_into_box_per90 DOUBLE,
  through_balls_completed_per90 DOUBLE
);

CREATE TABLE IF NOT EXISTS team_season_stats (
  team_id VARCHAR, league VARCHAR, season VARCHAR,
  minutes INT, possession_pct DOUBLE,
  passes_per90 DOUBLE, progressive_passes_per90 DOUBLE,
  progressive_carries_per90 DOUBLE,
  crosses_into_box_per90 DOUBLE, through_balls_completed_per90 DOUBLE,
  opp_passes_allowed_per_def_action DOUBLE,
  pressures_att3rd_per90 DOUBLE,
  aerials_win_pct DOUBLE
);

CREATE TABLE IF NOT EXISTS derived_player_vectors (
  player_id VARCHAR, league VARCHAR, season VARCHAR, pos VARCHAR, vec JSON
);

CREATE TABLE IF NOT EXISTS derived_team_vectors (
  team_id VARCHAR, league VARCHAR, season VARCHAR, vec JSON
);

CREATE TABLE IF NOT EXISTS feature_order (
  feature VARCHAR, ord INT
);
'''

FEATURES = [
    ("poss_pct",0),("prog_passes",1),("prog_carries",2),("crosses_box",3),
    ("throughballs",4),("press_intensity",5),("aerials_win_pct",6)
]

if __name__ == "__main__":
    con = duckdb.connect("duckdb/fit.db")
    con.execute(DDL)
    # seed feature order for radar
    con.execute("DELETE FROM feature_order")
    con.executemany("INSERT INTO feature_order VALUES (?,?)", FEATURES)
    con.close()
    print("✅ DuckDB initialized at duckdb/fit.db")
