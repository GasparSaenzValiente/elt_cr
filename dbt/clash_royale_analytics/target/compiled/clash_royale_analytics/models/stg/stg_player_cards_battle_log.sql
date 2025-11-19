with source_data as (
    select * from "cr_db"."public"."landing_player_cards_battle_log"
)
select distinct on (battle_id, player_tag, card_id)
    cast(battle_id as varchar(64)) as battle_id,
    cast(player_tag as varchar(10)) as battle_player_tag,
    card_id as battle_player_card_id,
    cast(card_level as smallint) as battle_player_card_level,
    cast(snapshot_date as date) as snapshot_date
from source_data
order by battle_id, player_tag, card_id