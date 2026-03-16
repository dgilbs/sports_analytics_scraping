
      update "neondb"."fotmob"."fantasy_roster_2026_snapshot"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "fantasy_roster_2026_snapshot__dbt_tmp143848118986" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "neondb"."fotmob"."fantasy_roster_2026_snapshot".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and "neondb"."fotmob"."fantasy_roster_2026_snapshot".dbt_valid_to is null;
      


    insert into "neondb"."fotmob"."fantasy_roster_2026_snapshot" ("manager", "player", "player_id", "db_name", "team", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."manager",DBT_INTERNAL_SOURCE."player",DBT_INTERNAL_SOURCE."player_id",DBT_INTERNAL_SOURCE."db_name",DBT_INTERNAL_SOURCE."team",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "fantasy_roster_2026_snapshot__dbt_tmp143848118986" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  