with source_data as (
    select * from "cr_db"."public"."landing_clans"
)
select 
    cast(tag as varchar(10)) as clan_tag,
    cast(name as varchar(10)) as clan_name,
    cast(type as varchar(11)) as clan_type,
    clan_score,
    clan_war_trophies,
    cast(required_trophies as smallint) as clan_required_trophies,
    cast(members as smallint) as clan_members,
    cast(location_id as int) as clan_location_id,
    cast(location_name as varchar(50)) as clan_location_name,
    cast(snapshot_date as date) as snapshot_date
from source_data