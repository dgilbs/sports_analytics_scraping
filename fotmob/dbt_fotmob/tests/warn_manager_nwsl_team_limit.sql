{{ config(severity='warn') }}

-- Warns when a manager has more than 2 active (non-benched, played) players
-- from the same NWSL team in a given fantasy week.

select
    fantasy_week,
    manager,
    team_name,
    count(*)                                                        as player_count,
    string_agg(player_name, ', ' order by player_name)             as players
from {{ ref('fantasy_roster_match_points_2026') }}
where is_benched = false
  and minutes_played > 0
  and team_name is not null
group by fantasy_week, manager, team_name
having count(*) > 2
