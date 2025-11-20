select distinct
    game_mode_id,
    game_mode_name
from {{ ref('stg_battle_info') }}