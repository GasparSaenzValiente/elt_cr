with source_data as (
    select * from {{ source('cr_db_landing', 'landing_player_cards_battle_log') }}
)
select 
    cast(battle_id as varchar(64)) as battle_id,
    cast(player_tag as varchar(10)) as battle_player_tag,
    card_id as battle_player_card_id,
    cast(card_name as varchar(50)) as battle_player_card_name,
    cast(card_level as smallint) as battle_player_card_level,
    cast(snapshot_date as date) as snapshot_date
from source_data