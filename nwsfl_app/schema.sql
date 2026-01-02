-- ============================================
-- LEAGUES & TEAMS (New)
-- ============================================

CREATE TABLE nwsfl.leagues (
    league_id SERIAL PRIMARY KEY,
    league_name VARCHAR(100) NOT NULL,
    season INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE nwsfl.teams (
    team_id SERIAL PRIMARY KEY,
    league_id INT NOT NULL REFERENCES leagues(league_id),
    user_id VARCHAR(50) NOT NULL,  -- Must match username in config.yaml
    team_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(league_id, user_id)  -- One team per user per league
);

-- ============================================
-- PLAYERS (You probably have this already)
-- ============================================

CREATE TABLE nwsfl.players (
    player_id SERIAL PRIMARY KEY,
    player_name VARCHAR(100) NOT NULL,
    position VARCHAR(20),  -- GK, DEF, MID, FWD
    team VARCHAR(50),  -- NWSL team (e.g., "Portland Thorns")
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- ROSTERS (New)
-- ============================================

CREATE TABLE nwsfl.rosters (
    roster_id SERIAL PRIMARY KEY,
    team_id INT NOT NULL REFERENCES teams(team_id),
    player_id INT NOT NULL REFERENCES players(player_id),
    drafted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_id, player_id)  -- Can't draft same player twice
);

-- Index for faster queries
CREATE INDEX idx_rosters_team ON rosters(team_id);
CREATE INDEX idx_rosters_player ON rosters(player_id);

-- ============================================
-- LINEUPS (New)
-- ============================================

CREATE TABLE nwsfl.lineups (
    lineup_id SERIAL PRIMARY KEY,
    team_id INT NOT NULL REFERENCES teams(team_id),
    matchweek INT NOT NULL,
    player_id INT NOT NULL REFERENCES players(player_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_id, matchweek, player_id)  -- Can't start same player twice in one week
);

-- Index for faster queries
CREATE INDEX idx_lineups_team_week ON lineups(team_id, matchweek);

-- ============================================
-- PLAYER SCORES (From your dbt model - adjust to match)
-- ============================================

CREATE TABLE nwsfl.player_scores (
    score_id SERIAL PRIMARY KEY,
    player_id INT NOT NULL REFERENCES players(player_id),
    matchweek INT NOT NULL,
    match_date DATE,
    
    -- Stats (adjust these to match your dbt output)
    minutes_played INT DEFAULT 0,
    goals INT DEFAULT 0,
    assists INT DEFAULT 0,
    clean_sheet BOOLEAN DEFAULT false,
    saves INT DEFAULT 0,
    yellow_cards INT DEFAULT 0,
    red_cards INT DEFAULT 0,
    
    -- Calculated fantasy points
    points DECIMAL(5,2) DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, matchweek)
);

CREATE INDEX idx_player_scores_week ON player_scores(matchweek);
CREATE INDEX idx_player_scores_player ON player_scores(player_id);

-- ============================================
-- LEAGUE STANDINGS (Computed view or table)
-- ============================================

-- Option 1: As a materialized view (refreshed by your dbt model)
CREATE MATERIALIZED VIEW nwsfl.league_standings AS
SELECT 
    t.team_id,
    t.league_id,
    t.team_name,
    t.user_id,
    COALESCE(SUM(ps.points), 0) as total_points,
    COUNT(DISTINCT l.matchweek) as weeks_played
FROM teams t
LEFT JOIN lineups l ON t.team_id = l.team_id
LEFT JOIN player_scores ps ON l.player_id = ps.player_id AND l.matchweek = ps.matchweek
GROUP BY t.team_id, t.league_id, t.team_name, t.user_id
ORDER BY total_points DESC;

-- Refresh this view weekly after your dbt run
-- REFRESH MATERIALIZED VIEW league_standings;

-- Option 2: As a regular table (populated by dbt)
CREATE TABLE nwsfl.league_standings (
    standing_id SERIAL PRIMARY KEY,
    team_id INT NOT NULL REFERENCES teams(team_id),
    league_id INT NOT NULL REFERENCES leagues(league_id),
    total_points DECIMAL(10,2) DEFAULT 0,
    weeks_played INT DEFAULT 0,
    rank INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(league_id, team_id)
);