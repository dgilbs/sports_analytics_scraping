# NWSFL Fantasy Weekly Update Process

Run this process after each set of matches to update fantasy points.

---

## Prerequisites

- Chrome running with remote debugging: `--remote-debugging-port=9222`
- Python environment activated: `source dbt_env/bin/activate`
- dbt profile configured (`~/.dbt/profiles.yml` pointing to Neon)

---

## Step 1 — Set the Date Range

In `scraping_code.ipynb`, update the date filter cell:

```python
sd = date(year=2026, month=3, day=27)
ed = date(year=2026, month=3, day=29)
```

Run that cell. This filters `matches_df` to the relevant matches and writes their IDs to `data/active_match_ids.txt`.

---

## Step 2 — Scrape Match Data

Run the scraping cell in `scraping_code.ipynb`:

```python
long_all = await scrape_many_matches(page, match_url_map, arr, sleep_s=0.8)
```

This saves raw player stats to `raw_data/match_reports/<match_id>.pkl`.

Then run the lineup scraping cell:

```python
await scrape_lineups_fast(page, match_ids, overwrite=True)
```

This saves lineup data to `data/lineups/<match_id>.pkl`.

---

## Step 3 — Clean Raw Data

Run the **Clean Raw Data** cell in `scraping_code.ipynb`.

- Automatically filtered to active match IDs
- Outputs cleaned CSVs to `data/match_reports/<match_id>.csv`

---

## Step 4 — Load to Database

```bash
python fotmob/load_to_db.py
```

- Automatically filtered to active match IDs via `data/active_match_ids.txt`
- Loads lineups and player stats into Neon Postgres

---

## Step 5 — Fill in Goals Conceded

Open `dbt_fotmob/seeds/team_goals_conceded_2026.csv` and fill in the `goals_conceded` column for the active matches. The file includes `team_name` and `match_date` columns to help identify rows.

Then seed the data:

```bash
cd fotmob/dbt_fotmob && dbt seed -s team_goals_conceded_2026
```

---

## Step 6 — Fill in Cards

**6a.** Run the **pre-fill cards staging** cell in `scraping_code.ipynb`.

This reads lineup pkl files and writes `dbt_fotmob/seeds/player_cards_2026_staging.csv` with every player from the active matches, all defaulting to `0` cards.

**6b.** Open `player_cards_2026_staging.csv` and change `yellow_cards` / `red_cards` for any player who received a card. Leave all others as `0`.

**6c.** Run the **resolver** cell in `scraping_code.ipynb`.

This merges the new card data into `player_cards_2026.csv`, preserving previous weeks' data.

**6d.** Seed the data:

```bash
dbt seed -s player_cards_2026
```

---

## Step 7 — Run dbt

```bash
dbt run
```

This rebuilds all fantasy points models.

---

## Step 8 — Handle Roster Changes (if any)

### Fantasy transfers
Add rows to `dbt_fotmob/seeds/transfers_2026.csv`:

```
transfer_date,manager,player_out,player_in,is_sei,is_d45,is_transfer_window
2026/04/01,Bill,Rose Lavelle,Sofia Huerta,No,No,No
```

For trades between two managers, add one row per manager.

### Bench decisions
Update `dbt_fotmob/seeds/fantasy_weekly_benches_2026.csv` with any players benched this week.

### Real-life team trades
Update the `team` column in `dbt_fotmob/seeds/nwsfl_roster_matched.csv` for the affected player. Points will automatically use the new team once new match data is loaded.

After any seed changes:

```bash
dbt seed && dbt run
```

---

## Step 9 — Verify

Run the dbt test to check no manager has more than 2 active players from the same NWSL team:

```bash
dbt test --select warn_manager_nwsl_team_limit
```

Query to spot-check points:

```sql
SELECT player_name, total_points, manager, is_benched
FROM fotmob.fantasy_roster_match_points_2026
WHERE fantasy_week = <week>
ORDER BY manager, total_points DESC;
```

---

## Fantasy Week Schedule

| Week | Start | End |
|------|-------|-----|
| 1 | 2026-03-12 | 2026-03-15 |
| 2 | 2026-03-20 | 2026-03-23 |
| 3 | 2026-03-25 | 2026-03-26 |
| 4 | 2026-03-27 | 2026-03-29 |

Add new weeks to `dbt_fotmob/seeds/fantasy_weeks_2026.csv` as the season progresses.
