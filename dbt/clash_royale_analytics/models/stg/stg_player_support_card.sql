with source_data as (
    select * from {{ source('cr_db_landing', 'landing_player_support_card') }}
)
select 
    cast(player_tag as varchar(10)) as player_tag,
    cast(spp_id as int) as player_support_id,
    cast(spp_name as varchar(50)) as player_support_name,
    cast(spp_level as smallint) as player_support_level,
    year,
    month,
    day,
    cast(snapshot_date as date) as snapshot_date
from source_data