# Draft Models

Two views built to support fantasy draft preparation. Both are in the `fotmob` schema and pull from `fantasy_match_points` and `fantasy_season_points`.

---

## `fotmob.draft_rankings`

One row per player. The primary view to rank and compare players within and across positions.

### Columns

| Column | Description |
|---|---|
| `draft_position` | Player position: GK, DF, MF, FW |
| `player_name` | Player name |
| `matches_played` | Matches with >0 minutes in 2025 |
| `season_total` | Total fantasy points for the season |
| `avg_pts_per_match` | Average points per appearance |
| `points_per_90` | Points per 90 minutes (from `fantasy_season_points`) |
| `ceiling` | Best single-match score |
| `floor` | Worst single-match score |
| `consistency_pct` | % of matches where points > 0 |
| `boom_rate_pct` | % of matches where points >= 10 |
| `bust_rate_pct` | % of matches where points < 0 |
| `pts_stddev` | Standard deviation of per-match points (lower = more predictable) |
| `position_rank` | Rank within position by `avg_pts_per_match` |
| `last_match` | Date of most recent appearance |

### Suggested Queries

**Best all-around per position (sorted by avg points):**
```sql
select * from fotmob.draft_rankings
where draft_position = 'GK'
order by position_rank;
```

**Safest floor picks — consistent, low bust rate:**
```sql
select * from fotmob.draft_rankings
order by consistency_pct desc, avg_pts_per_match desc;
```

**High upside / boom picks:**
```sql
select * from fotmob.draft_rankings
order by ceiling desc;
```

**Flex spot — best avg points regardless of position:**
```sql
select * from fotmob.draft_rankings
order by avg_pts_per_match desc
limit 20;
```

**Avoid — high bust rate:**
```sql
select * from fotmob.draft_rankings
where bust_rate_pct > 40
order by bust_rate_pct desc;
```

---

## `fotmob.draft_position_stats`

One row per player. Complements `draft_rankings` with position-specific underlying stats to understand *why* a player scores well — useful for spotting players whose fantasy points may understate or overstate their true value.

### Columns

| Column | Positions | Description |
|---|---|---|
| `draft_position` | All | GK, DF, MF, FW |
| `player_name` | All | Player name |
| `matches_played` | All | Matches with >0 minutes |
| `avg_minutes` | All | Average minutes per match |
| `goals` | All | Total goals scored |
| `assists` | All | Total assists |
| `clean_sheet_pct` | All | % of matches team kept a clean sheet |
| `avg_saves` | GK | Average saves per match |
| `saves_per_90` | GK | Saves per 90 minutes |
| `avg_tackles` | DF, MF, FW | Average tackles won per match |
| `avg_interceptions` | DF, MF, FW | Average interceptions per match |
| `avg_blocks` | DF, MF, FW | Average blocks per match |
| `avg_chances_created` | MF, FW | Average chances created per match |
| `avg_takeons` | MF, FW | Average successful take-ons per match |
| `pass_bonus_pct` | All | % of matches earning the >85% pass completion bonus |
| `touches_bonus_pct` | All | % of matches earning the >60 touches bonus |
| `goal_contributions_rank` | All | Rank within position by goals + assists |

### Suggested Queries

**GK — best shot-stoppers:**
```sql
select player_name, avg_saves, saves_per_90, clean_sheet_pct
from fotmob.draft_position_stats
where draft_position = 'GK'
order by avg_saves desc;
```

**DF — defensive workhorses:**
```sql
select player_name, clean_sheet_pct, avg_tackles, avg_interceptions, avg_blocks
from fotmob.draft_position_stats
where draft_position = 'DF'
order by clean_sheet_pct desc;
```

**MF/FW — attacking contributors:**
```sql
select player_name, goals, assists, avg_chances_created, avg_takeons
from fotmob.draft_position_stats
where draft_position in ('MF', 'FW')
order by goals + assists desc;
```

**Bonus hunters — players who reliably hit the pass completion and touches thresholds:**
```sql
select player_name, draft_position, pass_bonus_pct, touches_bonus_pct
from fotmob.draft_position_stats
order by pass_bonus_pct + touches_bonus_pct desc;
```

---

## Notes

- Both views only include players who appear in `player_id_mapping` with a `draft_position` set in `draft_list`.
- Players added from the 2025 season who are not in the 2026 draft need a position filled in `draft_list.csv` before they appear here.
- Data is based on the 2025 NWSL season. Yellow/red card data comes from FBRef via `fbref_fotmob_crossref` — players not in that crossref will have 0 for card-related deductions.
