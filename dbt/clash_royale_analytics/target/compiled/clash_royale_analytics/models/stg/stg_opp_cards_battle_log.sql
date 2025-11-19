with source_data as (
    select * from "cr_db"."public"."landing_opp_cards_battle_log"
)
select distinct on (battle_id, opp_tag, card_id)
    cast(battle_id as varchar(64)) as battle_id,
    cast(opp_tag as varchar(10)) as battle_opp_tag,
    card_id as battle_opp_card_id,
    cast(card_level as smallint) as battle_opp_card_level,
    cast(snapshot_date as date) as snapshot_date
from source_data
order by battle_id, opp_tag, card_id, snapshot_date