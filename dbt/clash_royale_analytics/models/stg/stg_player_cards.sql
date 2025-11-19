with source_data as (
    select * from {{ source('cr_db_landing', 'landing_player_cards') }}
)
select distinct on (player_tag, card_id, snapshot_date)
    cast(player_tag as varchar(10)) as player_tag,
    cast(card_id as int) as player_card_id,
    cast(card_level as smallint) as player_card_level,
    coalesce(cast(card_evolution_level as smallint), 0) as player_card_evolution_level,
    year,
    month,
    day,
    cast(snapshot_date as date) as snapshot_date
from source_data
order by player_tag, card_id, snapshot_date