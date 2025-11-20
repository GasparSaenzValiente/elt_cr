with standard_cards as (
    select
        player_tag,
        player_card_id as card_id,
        player_card_level as card_level,
        player_card_evolution_level,
        snapshot_date,
        'Standard' as card_type_category
    from {{ ref('stg_player_cards') }}
),

support_cards as (
    select
        player_tag,
        player_support_id as card_id,
        player_support_level as card_level,
        0 as player_card_evolution_level,
        snapshot_date,
        'Support' as card_type_category
    from {{ ref('stg_player_support_card') }}
),

unioned_holdings as (
    select * from standard_cards
    union all
    select * from support_cards
)

select 
    {{ dbt_utils.generate_surrogate_key(['player_tag', 'card_id', 'snapshot_date']) }} as holding_key,
    player_tag,
    card_id,
    snapshot_date,
    card_level,
    card_type_category

from unioned_holdings