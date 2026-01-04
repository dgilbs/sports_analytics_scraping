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
-- ROSTERS - Pulled from season_fantasy_rosters
-- ============================================
-- Note: The nwsfl.rosters table is synced from the season_fantasy_rosters table in Neon
-- This automatically populates team rosters for each NWSFL team
-- No need for manual inserts - the data comes from the fantasy platform

-- If you need to manually populate from season_fantasy_rosters, use:
-- INSERT INTO nwsfl.rosters (team_id, player_id)
-- SELECT nwsfl_team_id, player_id FROM season_fantasy_rosters
-- WHERE season = '2025'
-- ON CONFLICT (team_id, player_id) DO NOTHING;

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

