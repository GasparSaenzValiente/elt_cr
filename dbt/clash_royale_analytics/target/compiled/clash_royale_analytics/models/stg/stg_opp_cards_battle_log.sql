with source_data as (
    select * from "cr_db"."public"."landing_opp_cards_battle_log"
)
select 
    cast(battle_id as varchar(64)) as battle_id,
    cast(opp_tag as varchar(10)) as battle_opp_tag,
    card_id as battle_opp_card_id,
    cast(card_name as varchar(50)) as battle_opp_card_name,
    cast(card_level as smallint) as battle_opp_card_level
from source_data