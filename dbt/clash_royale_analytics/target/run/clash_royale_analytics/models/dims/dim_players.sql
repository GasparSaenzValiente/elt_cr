
  create view "cr_db"."public"."dim_players__dbt_tmp"
    
    
  as (
    with source_data as (
    select * from "cr_db"."public"."stg_players"
)
select * from source_data
  );