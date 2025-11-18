
  create view "cr_db"."public"."stg_clan_members__dbt_tmp"
    
    
  as (
    with source_data as (
    select * from "cr_db"."public"."landing_members"
)
select 
    cast(clan_tag as varchar(10)) as clan_tag,
    cast(member_tag as varchar(10)) as member_tag,
    cast(member_name as varchar(15)) as member_name,
    cast(member_role as varchar(25)) as member_role,
    cast(member_clan_rank as smallint) as member_rank,
    cast(member_exp_level as smallint) as member_exp_level,
    member_donations,
    donations_received as member_donations_received,
    year,
    month,
    day,
    cast(snapshot_date as date) as snapshot_date
from source_data
  );