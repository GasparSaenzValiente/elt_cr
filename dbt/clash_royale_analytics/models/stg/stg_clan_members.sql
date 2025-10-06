with source_data as (
    select * from {{ source('cr_db_landing', 'landing_members') }}
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
    day
from source_data