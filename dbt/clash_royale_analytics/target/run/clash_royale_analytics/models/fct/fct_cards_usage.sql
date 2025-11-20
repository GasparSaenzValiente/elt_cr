
  
    

  create  table "cr_db"."public"."fct_cards_usage__dbt_tmp"
  
  
    as
  
  (
    

with player_cards as (
    select
        battle_id,
        battle_player_tag as player_tag,
        battle_player_card_id as card_id,
        battle_player_card_level as card_level,
        snapshot_date,
        'Player' as played_by
    from "cr_db"."public"."stg_player_cards_battle_log"
),

opponent_cards as (
    select
        battle_id,
        battle_opp_tag as player_tag,
        battle_opp_card_id as card_id,
        battle_opp_card_level as card_level,
        snapshot_date,
        'Opponent' as played_by
    from "cr_db"."public"."stg_opp_cards_battle_log"
),

unioned_usage as (
    select * from player_cards
    union all
    select * from opponent_cards
)

select
    -- 🗝️ Surrogate Key OBLIGATORIA (3 columnas para unicidad)
    md5(cast(coalesce(cast(battle_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(player_tag as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(card_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as usage_key,
    
    battle_id,
    player_tag,
    card_id,
    card_level,
    played_by,
    snapshot_date
    
from unioned_usage
  );
  