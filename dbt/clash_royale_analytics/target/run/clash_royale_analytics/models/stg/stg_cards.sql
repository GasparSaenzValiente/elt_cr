
  create view "cr_db"."public"."stg_cards__dbt_tmp"
    
    
  as (
    with source_data as (
    select * from "cr_db"."public"."landing_cards"
)
select 
    card_id,
    cast(card_name as varchar(50)) as card_name,
    cast(rarity as varchar(9)) as card_rarity,
    cast(max_level as smallint) as card_max_level,
    cast(elixir_cost as smallint) as card_elixir_cost,
    coalesce(cast(max_evolution_level as smallint), 0) as card_max_evolution_level
from source_data
order by card_id
  );