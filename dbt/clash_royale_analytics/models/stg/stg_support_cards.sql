with source_data as (
    select * from {{ source('cr_db_landing', 'landing_support_cards') }}
)
select 
    card_id as support_id,
    cast(card_name as varchar(50)) as support_name,
    cast(rarity as varchar(9)) as support_rarity,
    cast(max_level as smallint) as support_max_level,
    cast(elixir_cost as smallint) as support_elixir_cost,
    cast(max_evolution_level as smallint) as support_max_evolution_level
from source_data