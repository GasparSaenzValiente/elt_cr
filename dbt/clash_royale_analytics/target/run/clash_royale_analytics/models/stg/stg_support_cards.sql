
  create view "cr_db"."public"."stg_support_cards__dbt_tmp"
    
    
  as (
    with source_data as (
    select * from "cr_db"."public"."landing_support_cards"
)
select distinct on (card_id)
    card_id as support_id,
    cast(card_name as varchar(50)) as support_name,
    cast(rarity as varchar(9)) as support_rarity,
    cast(max_level as smallint) as support_max_level,
    coalesce(cast(elixir_cost as smallint),0) as support_elixir_cost,
    coalesce(cast(max_evolution_level as smallint), 0) as support_max_evolution_level
from source_data
  );