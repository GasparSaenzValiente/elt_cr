with clans as (
    select
        *,
        row_number() over (
            partition by clan_tag
            order by snapshot_date desc
        ) as rn
    from "cr_db"."public"."stg_clans"
)
select
    clan_tag,
    clan_name,
    clan_type,
    clan_score,
    clan_war_trophies,
    clan_required_trophies,
    clan_members,
    location_id,
    location_name
from clans
where rn = 1