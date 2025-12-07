-- SQL schema for sports_rag DuckDB
-- Define tables here
CREATE TABLE IF NOT EXISTS leagues (
    league_id INTEGER,
    name VARCHAR,
    country VARCHAR,
    season VARCHAR,
    PRIMARY KEY (league_id, season)
);

CREATE TABLE IF NOT EXISTS teams (
    team_id INTEGER,
    name VARCHAR,
    league_id INTEGER,
    season VARCHAR,
    PRIMARY KEY (team_id, season),
    FOREIGN KEY (league_id, season) REFERENCES leagues (league_id, season)
);

CREATE TABLE IF NOT EXISTS players (
    player_id INTEGER PRIMARY KEY,
    name VARCHAR,
    team_id INTEGER,
    season VARCHAR,
    position VARCHAR,
    FOREIGN KEY (team_id, season) REFERENCES teams (team_id, season)
);

CREATE TABLE IF NOT EXISTS matches (
    match_id INTEGER PRIMARY KEY,
    league_id INTEGER,
    season VARCHAR,
    home_team_id INTEGER,
    away_team_id INTEGER,
    match_date DATE,
    FOREIGN KEY (league_id, season) REFERENCES leagues (league_id, season),
    FOREIGN KEY (home_team_id, season) REFERENCES teams (team_id, season),
    FOREIGN KEY (away_team_id, season) REFERENCES teams (team_id, season)
);

CREATE TABLE IF NOT EXISTS player_match_stats (
    match_id INTEGER,
    player_id INTEGER,
    team_id INTEGER,
    minutes INTEGER,
    goals INTEGER,
    assists INTEGER,
    shots INTEGER,
    xg DOUBLE,
    passes_completed INTEGER,
    passes_attempted INTEGER,
    progressive_passes INTEGER,
    PRIMARY KEY (match_id, player_id),
    FOREIGN KEY (match_id) REFERENCES matches (match_id),
    FOREIGN KEY (player_id) REFERENCES players (player_id)
);

SELECT
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default
FROM
    information_schema.columns
WHERE
    table_name = 'leagues';