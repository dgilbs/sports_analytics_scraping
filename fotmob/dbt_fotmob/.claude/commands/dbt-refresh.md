Run the full pipeline in order:

1. `python /Users/dgilberg/Documents/sports_analytics_scraping/fotmob/match_nwsfl_rosters.py`
2. `dbt seed`
3. `dbt snapshot`
4. `dbt run`

Run each command sequentially, stopping and reporting the error if any step fails.
