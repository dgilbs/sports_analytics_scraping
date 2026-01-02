# Fantasy Player Positions - 2025 Season

## ✅ Successfully Loaded!

**Database Table:** `nwsfl.fantasy_player_positions`

### Summary

- **Season:** 2025
- **Total Players:** 301
- **Player Data Source:** FBRef (player_id matches `dim_players.id`)

### Position Breakdown

| Position | Count | Description |
|----------|-------|-------------|
| GK | 30 | Goalkeepers |
| DF | 96 | Defenders |
| MF | 88 | Midfielders |
| FW | 87 | Forwards |

### Data Source

**File:** `nwsfl_dbt/seeds/nwsfl_fantasy_players_2025.csv`

This CSV maps NWSL players to their fantasy positions for the 2025 season. Each player has:
- `squad` - Current NWSL team
- `player_id` - FBRef player identifier (matches `dim_players.id`)
- `player` - Player name
- `fantasy_position` - Fantasy league position (GK/DF/MF/FW)

### Notes

**Players Handled:**
- ✓ 301/305 players loaded successfully
- ✓ 1 duplicate resolved (Cecelia Kizer - traded from Gotham FC to Royals, using Royals)
- ✓ 1 typo fixed (GW → GK for Melissa Lowder)
- ⚠️ 4 players not found in dim_players (may be new/updated in FBRef)
- ⚠️ 20 players skipped (no fantasy position assigned in CSV)

**Missing Players (not in dim_players):**
1. Hannah Stambaugh (b58f71f5)
2. Melissa Lowder (8223d6eb)
3. Charlotte Mclean (444cfc74)
4. Allie George (07b899ab)

These players need to be added to `dim_players` if they should be available for fantasy.

### Database Schema

```sql
CREATE TABLE nwsfl.fantasy_player_positions (
    id SERIAL PRIMARY KEY,
    player_id VARCHAR(50) NOT NULL,  -- fbref ID
    season INT NOT NULL,
    fantasy_position VARCHAR(5) NOT NULL,  -- GK, DF, MF, FW
    nwsl_team VARCHAR(50),  -- Current team
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, season)
);
```

### Useful Queries

**Get all available players by position:**
```sql
SELECT p.player, fpp.nwsl_team, fpp.fantasy_position
FROM nwsfl.fantasy_player_positions fpp
JOIN nwsfl.dim_players p ON fpp.player_id = p.id
WHERE fpp.season = 2025
  AND fpp.fantasy_position = 'FW'  -- Change to GK/DF/MF/FW
ORDER BY p.player;
```

**Count players by team and position:**
```sql
SELECT 
    nwsl_team,
    fantasy_position,
    COUNT(*) as player_count
FROM nwsfl.fantasy_player_positions
WHERE season = 2025
GROUP BY nwsl_team, fantasy_position
ORDER BY nwsl_team, fantasy_position;
```

**Find players not yet on any fantasy roster:**
```sql
SELECT p.player, fpp.nwsl_team, fpp.fantasy_position
FROM nwsfl.fantasy_player_positions fpp
JOIN nwsfl.dim_players p ON fpp.player_id = p.id
LEFT JOIN nwsfl.rosters r ON p.id = r.player_id
WHERE fpp.season = 2025
  AND r.roster_id IS NULL
ORDER BY fpp.fantasy_position, p.player;
```

### Integration with Fantasy League

This table can be used to:
1. **Player Selection** - Show available players by position for drafting
2. **Position Validation** - Ensure players are assigned to correct fantasy positions
3. **Roster Constraints** - Validate fantasy roster composition (e.g., must have 1 GK, 4 DF, etc.)
4. **Weekly Lineups** - Verify starting lineup has correct position distribution

### Files Created

- `schema_fantasy_positions.sql` - Table creation script
- `load_fantasy_positions.py` - Data loading script  
- `nwsfl_dbt/seeds/nwsfl_fantasy_players_2025.csv` - Source data (safe to commit)

### Updating Data

To reload or update player positions:
```bash
# Edit the CSV file
nano nwsfl_dbt/seeds/nwsfl_fantasy_players_2025.csv

# Reload data (will delete old 2025 data and insert new)
python load_fantasy_positions.py
```

### Next Season

For the 2026 season:
1. Create a new CSV: `nwsfl_fantasy_players_2026.csv`
2. Update the script to load 2026 data
3. Players can have different positions in different seasons!


