with source_data as (
    select * from {{ source('cr_db_staging', 'stg_players') }}
)
select
    cast(tag as varchar(10)) as player_tag,
    cast(tag as varchar(10)) as player_name,
    exp_level,
    trophies,
    
from source_data
