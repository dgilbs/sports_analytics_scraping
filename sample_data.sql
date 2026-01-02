-- ============================================
-- SAMPLE DATA INSERTS FOR NWSFL APP
-- ============================================
-- Run these queries after creating your schema to populate test data
-- ============================================

-- ============================================
-- LEAGUES
-- ============================================
INSERT INTO nwsfl.leagues (league_name, season) VALUES
    ('Friends League', 2025),
    ('Elite League', 2025),
    ('Casual Sunday League', 2025);

-- ============================================
-- TEAMS
-- ============================================
-- Note: user_id must match usernames in your config (alice, bob, charlie)
INSERT INTO nwsfl.teams (league_id, user_id, team_name) VALUES
    -- Friends League (league_id = 1)
    (1, 'alice', 'Alice''s Thorns'),
    (1, 'bob', 'Bob''s Spirit'),
    (1, 'charlie', 'Charlie''s Reign'),
    
    -- Elite League (league_id = 2)
    (2, 'alice', 'Alice''s Warriors'),
    (2, 'bob', 'Bob''s Pride');

-- ============================================
-- PLAYERS
-- ============================================
INSERT INTO nwsfl.players (player_name, position, team, active) VALUES
    -- Forwards
    ('Sophia Smith', 'FWD', 'Portland Thorns', true),
    ('Trinity Rodman', 'FWD', 'Washington Spirit', true),
    ('Mallory Swanson', 'FWD', 'Chicago Red Stars', true),
    ('Alex Morgan', 'FWD', 'San Diego Wave', true),
    ('Megan Rapinoe', 'FWD', 'OL Reign', true),
    ('Lynn Williams', 'FWD', 'NJ/NY Gotham FC', true),
    
    -- Midfielders
    ('Rose Lavelle', 'MID', 'OL Reign', true),
    ('Lindsey Horan', 'MID', 'Lyon', true),
    ('Sam Coffey', 'MID', 'Portland Thorns', true),
    ('Ashley Sanchez', 'MID', 'Washington Spirit', true),
    ('Andi Sullivan', 'MID', 'Washington Spirit', true),
    ('Croix Bethune', 'MID', 'Washington Spirit', true),
    
    -- Defenders
    ('Naomi Girma', 'DEF', 'San Diego Wave', true),
    ('Emily Fox', 'DEF', 'Arsenal', true),
    ('Becky Sauerbrunn', 'DEF', 'Portland Thorns', true),
    ('Tierna Davidson', 'DEF', 'NJ/NY Gotham FC', true),
    ('Emily Sonnett', 'DEF', 'OL Reign', true),
    ('Abby Dahlkemper', 'DEF', 'San Diego Wave', true),
    
    -- Goalkeepers
    ('Alyssa Naeher', 'GK', 'Chicago Red Stars', true),
    ('Aubrey Kingsbury', 'GK', 'Washington Spirit', true),
    ('Adrianna Franch', 'GK', 'Kansas City Current', true),
    ('Casey Murphy', 'GK', 'NC Courage', true);

-- ============================================
-- ROSTERS
-- ============================================
-- Alice's team in Friends League
INSERT INTO nwsfl.rosters (team_id, player_id) VALUES
    (1, 1),   -- Sophia Smith
    (1, 2),   -- Trinity Rodman
    (1, 7),   -- Rose Lavelle
    (1, 9),   -- Sam Coffey
    (1, 13),  -- Naomi Girma
    (1, 15),  -- Becky Sauerbrunn
    (1, 19);  -- Alyssa Naeher

-- Bob's team in Friends League
INSERT INTO nwsfl.rosters (team_id, player_id) VALUES
    (2, 3),   -- Mallory Swanson
    (2, 4),   -- Alex Morgan
    (2, 8),   -- Lindsey Horan
    (2, 10),  -- Ashley Sanchez
    (2, 14),  -- Emily Fox
    (2, 16),  -- Tierna Davidson
    (2, 20);  -- Aubrey Kingsbury

-- Charlie's team in Friends League
INSERT INTO nwsfl.rosters (team_id, player_id) VALUES
    (3, 5),   -- Megan Rapinoe
    (3, 6),   -- Lynn Williams
    (3, 11),  -- Andi Sullivan
    (3, 12),  -- Croix Bethune
    (3, 17),  -- Emily Sonnett
    (3, 18),  -- Abby Dahlkemper
    (3, 21);  -- Adrianna Franch

-- Alice's team in Elite League
INSERT INTO nwsfl.rosters (team_id, player_id) VALUES
    (4, 1),   -- Sophia Smith
    (4, 7),   -- Rose Lavelle
    (4, 13),  -- Naomi Girma
    (4, 19);  -- Alyssa Naeher

-- Bob's team in Elite League
INSERT INTO nwsfl.rosters (team_id, player_id) VALUES
    (5, 2),   -- Trinity Rodman
    (5, 8),   -- Lindsey Horan
    (5, 14),  -- Emily Fox
    (5, 20);  -- Aubrey Kingsbury

-- ============================================
-- LINEUPS (Matchweek 1)
-- ============================================
-- Alice's starting lineup - Friends League
INSERT INTO nwsfl.lineups (team_id, matchweek, player_id) VALUES
    (1, 1, 1),   -- Sophia Smith
    (1, 1, 2),   -- Trinity Rodman
    (1, 1, 7),   -- Rose Lavelle
    (1, 1, 9),   -- Sam Coffey
    (1, 1, 13),  -- Naomi Girma
    (1, 1, 15),  -- Becky Sauerbrunn
    (1, 1, 19);  -- Alyssa Naeher

-- Bob's starting lineup - Friends League
INSERT INTO nwsfl.lineups (team_id, matchweek, player_id) VALUES
    (2, 1, 3),   -- Mallory Swanson
    (2, 1, 4),   -- Alex Morgan
    (2, 1, 8),   -- Lindsey Horan
    (2, 1, 10),  -- Ashley Sanchez
    (2, 1, 14),  -- Emily Fox
    (2, 1, 16),  -- Tierna Davidson
    (2, 1, 20);  -- Aubrey Kingsbury

-- Charlie's starting lineup - Friends League
INSERT INTO nwsfl.lineups (team_id, matchweek, player_id) VALUES
    (3, 1, 5),   -- Megan Rapinoe
    (3, 1, 6),   -- Lynn Williams
    (3, 1, 11),  -- Andi Sullivan
    (3, 1, 12),  -- Croix Bethune
    (3, 1, 17),  -- Emily Sonnett
    (3, 1, 18),  -- Abby Dahlkemper
    (3, 1, 21);  -- Adrianna Franch

-- ============================================
-- PLAYER SCORES (Matchweek 1)
-- ============================================
INSERT INTO nwsfl.player_scores (
    player_id, matchweek, match_date, minutes_played, goals, assists, 
    clean_sheet, saves, yellow_cards, red_cards, points
) VALUES
    -- Forwards
    (1, 1, '2025-03-15', 90, 2, 1, false, 0, 0, 0, 13.0),  -- Sophia Smith
    (2, 1, '2025-03-15', 90, 1, 2, false, 0, 0, 0, 11.0),  -- Trinity Rodman
    (3, 1, '2025-03-15', 90, 1, 0, false, 0, 1, 0, 6.0),   -- Mallory Swanson
    (4, 1, '2025-03-15', 75, 1, 1, false, 0, 0, 0, 10.0),  -- Alex Morgan
    (5, 1, '2025-03-15', 90, 0, 2, false, 0, 0, 0, 6.0),   -- Megan Rapinoe
    (6, 1, '2025-03-15', 80, 1, 0, false, 0, 0, 0, 8.0),   -- Lynn Williams
    
    -- Midfielders
    (7, 1, '2025-03-15', 90, 1, 1, false, 0, 0, 0, 10.0),  -- Rose Lavelle
    (8, 1, '2025-03-15', 90, 0, 2, false, 0, 0, 0, 6.0),   -- Lindsey Horan
    (9, 1, '2025-03-15', 90, 0, 1, false, 0, 0, 0, 4.0),   -- Sam Coffey
    (10, 1, '2025-03-15', 90, 0, 1, false, 0, 1, 0, 3.0),  -- Ashley Sanchez
    (11, 1, '2025-03-15', 90, 0, 0, false, 0, 0, 0, 2.0),  -- Andi Sullivan
    (12, 1, '2025-03-15', 85, 1, 0, false, 0, 0, 0, 8.0),  -- Croix Bethune
    
    -- Defenders
    (13, 1, '2025-03-15', 90, 0, 0, true, 0, 0, 0, 6.0),   -- Naomi Girma
    (14, 1, '2025-03-15', 90, 0, 1, false, 0, 0, 0, 4.0),  -- Emily Fox
    (15, 1, '2025-03-15', 90, 0, 0, true, 0, 0, 0, 6.0),   -- Becky Sauerbrunn
    (16, 1, '2025-03-15', 90, 0, 0, false, 0, 1, 0, 1.0),  -- Tierna Davidson
    (17, 1, '2025-03-15', 90, 0, 0, true, 0, 0, 0, 6.0),   -- Emily Sonnett
    (18, 1, '2025-03-15', 90, 0, 0, false, 0, 0, 0, 2.0),  -- Abby Dahlkemper
    
    -- Goalkeepers
    (19, 1, '2025-03-15', 90, 0, 0, true, 5, 0, 0, 10.0),  -- Alyssa Naeher
    (20, 1, '2025-03-15', 90, 0, 0, false, 3, 0, 0, 4.0),  -- Aubrey Kingsbury
    (21, 1, '2025-03-15', 90, 0, 0, true, 4, 0, 0, 9.0),   -- Adrianna Franch
    (22, 1, '2025-03-15', 90, 0, 0, false, 2, 0, 0, 3.0);  -- Casey Murphy

-- ============================================
-- LEAGUE STANDINGS (If using table instead of materialized view)
-- ============================================
-- This should be calculated/updated automatically, but here's sample data:
INSERT INTO nwsfl.league_standings (team_id, league_id, total_points, weeks_played) VALUES
    (1, 1, 67.0, 1),  -- Alice's Thorns (Friends League)
    (2, 1, 54.0, 1),  -- Bob's Spirit (Friends League)
    (3, 1, 48.0, 1),  -- Charlie's Reign (Friends League)
    (4, 2, 42.0, 1),  -- Alice's Warriors (Elite League)
    (5, 2, 35.0, 1);  -- Bob's Pride (Elite League)

-- ============================================
-- VERIFICATION QUERIES
-- ============================================
-- Uncomment these to verify your data was inserted correctly:

-- SELECT * FROM nwsfl.leagues;
-- SELECT * FROM nwsfl.teams;
-- SELECT * FROM nwsfl.players ORDER BY position, player_name;
-- SELECT t.team_name, p.player_name, p.position 
--   FROM nwsfl.rosters r 
--   JOIN nwsfl.teams t ON r.team_id = t.team_id 
--   JOIN nwsfl.players p ON r.player_id = p.player_id 
--   ORDER BY t.team_name, p.position;
-- SELECT * FROM nwsfl.lineups WHERE matchweek = 1;
-- SELECT * FROM nwsfl.player_scores WHERE matchweek = 1 ORDER BY points DESC;
-- SELECT * FROM nwsfl.league_standings ORDER BY league_id, rank;

