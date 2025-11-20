with cards as (
    select
        card_id,
        card_name,
        card_rarity,
        card_max_level,
        card_elixir_cost,
        card_max_evolution_level,
        'Standard' as card_type
    from {{ ref('stg_cards') }}
),

support_cards as (
    select
        support_id as card_id,
        support_name as card_name,
        support_rarity as card_rarity,
        support_max_level as card_max_level,
        support_elixir_cost as card_elixir_cost,
        support_max_evolution_level as card_max_evolution_level,
        'Support' as card_type
    from {{ ref('stg_support_cards') }}
),

unioned_data as (
    select * from cards
    union all
    select * from support_cards
)

select distinct on (card_id) *
from unioned_data
order by card_id