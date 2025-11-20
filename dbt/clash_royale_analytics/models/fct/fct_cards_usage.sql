{{ config(materialized='table') }}

with player_cards as (
    select
        battle_id,
        battle_player_tag as player_tag,
        battle_player_card_id as card_id,
        battle_player_card_level as card_level,
        snapshot_date,
        'Player' as played_by
    from {{ ref('stg_player_cards_battle_log') }}
),

opponent_cards as (
    select
        battle_id,
        battle_opp_tag as player_tag,
        battle_opp_card_id as card_id,
        battle_opp_card_level as card_level,
        snapshot_date,
        'Opponent' as played_by
    from {{ ref('stg_opp_cards_battle_log') }}
),

unioned_usage as (
    select * from player_cards
    union all
    select * from opponent_cards
)

select
    -- 🗝️ Surrogate Key OBLIGATORIA (3 columnas para unicidad)
    {{ dbt_utils.generate_surrogate_key(['battle_id', 'player_tag', 'card_id']) }} as usage_key,
    
    battle_id,
    player_tag,
    card_id,
    card_level,
    played_by,
    snapshot_date
    
from unioned_usage