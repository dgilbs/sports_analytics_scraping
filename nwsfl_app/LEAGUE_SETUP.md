# NWSFL 2025 League Setup

## ✅ League Created Successfully!

**League Details:**
- **Name:** NWSFL 2025
- **Season:** 2025
- **League ID:** 7
- **Total Teams:** 11

## 📋 Teams

| # | Team Name | Manager | User ID | Team ID |
|---|-----------|---------|---------|---------|
| 1 | The Justus League | Bill | bill | 7 |
| 2 | Triple Decaf FC | Noelle | noelle | 8 |
| 3 | Tika Tanaka | Joey | joey | 9 |
| 4 | Yazmanian Devils | Jackson | jackson | 10 |
| 5 | Midwest Express FC | Ryne | ryne | 11 |
| 6 | Iron Will FC | Mariam | mariam | 12 |
| 7 | Trying My Best FC | Danny | danny | 13 |
| 8 | Cal's Pals FC | Brock | brock | 14 |
| 9 | My Lil' Croixssant | Chris | chris | 15 |
| 10 | Emerald City | Nancy | nancy | 16 |
| 11 | Hal's Pals FC | Cameron | cameron | 17 |

## 🔐 Important: User Authentication

Each manager must have a matching user account in `config.yaml` with their `user_id`:
- bill
- noelle
- joey
- jackson
- ryne
- mariam
- danny
- brock
- chris
- nancy
- cameron

## 📊 Database Structure

The league was created in the following tables:
- `nwsfl.leagues` - League information (League ID: 7)
- `nwsfl.teams` - Team entries linked to league and users

## 🚀 Next Steps

1. **Add Users to config.yaml** (if not already done)
   - Each user_id above needs an entry in config.yaml
   - Use `scripts/generate_passwords.py` to create password hashes

2. **Build Player Rosters**
   ```sql
   INSERT INTO nwsfl.rosters (team_id, player_id)
   VALUES (7, player_id_here);  -- Assign players to teams
   ```

3. **Set Weekly Lineups**
   - Users can select their starting 11 via the Streamlit app
   - Data stored in `nwsfl.lineups` table

4. **Score Calculation**
   - Use dbt models to calculate fantasy points
   - Update `nwsfl.player_scores` table
   - Refresh `nwsfl.league_standings` view/table

## 🎯 Testing the App

1. Start the Streamlit app:
   ```bash
   streamlit run nwsfl_app.py
   ```

2. Log in with one of the manager user_ids
3. You should see "NWSFL 2025" as an available league
4. Select your team and manage your roster

## 📝 Files Created

- `create_nwsfl_2025_league.py` - Script used to create the league (added to .gitignore)
- `nwsfl_dbt/seeds/fantasy_teams.csv` - Updated with correct format for future reference

## 🔍 Quick Queries

View league standings:
```sql
SELECT t.team_name, t.user_id, COALESCE(SUM(ps.points), 0) as total_points
FROM nwsfl.teams t
LEFT JOIN nwsfl.lineups l ON t.team_id = l.team_id
LEFT JOIN nwsfl.player_scores ps ON l.player_id = ps.player_id 
WHERE t.league_id = 7
GROUP BY t.team_id, t.team_name, t.user_id
ORDER BY total_points DESC;
```

View team rosters:
```sql
SELECT t.team_name, p.player_name, p.position, p.team
FROM nwsfl.teams t
JOIN nwsfl.rosters r ON t.team_id = r.team_id
JOIN nwsfl.players p ON r.player_id = p.player_id
WHERE t.league_id = 7
ORDER BY t.team_name, p.position;
```

