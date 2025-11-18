with source_data as (
    select * from "cr_db"."public"."landing_player_cards"
)
select 
    cast(player_tag as varchar(10)) as player_tag,
    cast(card_id as int) as player_card_id,
    cast(card_name as varchar(50)) as player_card_name,
    cast(card_level as smallint) as player_card_level,
    cast(card_rarity as varchar(15)) as player_card_rarity,
    cast(card_evolution_level as smallint) as player_card_evolution_level,
    year,
    month,
    day,
    cast(snapshot_date as date) as snapshot_date
from source_data