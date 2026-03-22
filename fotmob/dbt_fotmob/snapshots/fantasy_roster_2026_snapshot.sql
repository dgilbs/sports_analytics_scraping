{% snapshot fantasy_roster_2026_snapshot %}

{{
    config(
        target_schema='fotmob',
        unique_key='player',
        strategy='check',
        check_cols=['manager', 'player_id'],
        invalidate_hard_deletes=true,
    )
}}

select
    manager,
    player,
    nullif(player_id::text, '')::bigint as player_id,
    db_name,
    team
from {{ ref('nwsfl_roster_matched') }}

{% endsnapshot %}
