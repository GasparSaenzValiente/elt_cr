with source_data as (
    select * from "cr_db"."public"."landing_clans"
)
select 
    cast(tag as varchar(10)) as clan_tag,
    cast(name as varchar(10)) as clan_name,
    cast(type as varchar(11)) as clan_type,
    cast("clanScore" as int) as clan_score,
    cast("clanWarTrophies" as int) as clan_war_trophies,
    cast("requiredTrophies" as smallint) as clan_required_trophies,
    cast(members as smallint) as clan_members,
    cast(location::json->>'id' AS int) AS location_id,
    cast(location::json->>'name' AS varchar(50)) AS location_name,
    cast(snapshot_date as date) as snapshot_date
from source_data