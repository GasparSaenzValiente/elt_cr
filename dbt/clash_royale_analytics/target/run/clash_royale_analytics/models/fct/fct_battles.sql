
  
    

  create  table "cr_db"."public"."fct_battles__dbt_tmp"
  
  
    as
  
  (
    with battles as (
    select * from "cr_db"."public"."stg_battle_info"
)

select
    md5(cast(coalesce(cast(battle_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(player_tag as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as battle_key,
    -- JOIN dims
    battle_id,
    player_tag,
    opp_tag,
    game_mode_id,
    
    -- JOIN dim_date
    snapshot_date, 
    battle_time,

    -- stg metrics
    player_crowns,
    opp_crowns,
    player_trophy_change,
    player_elixir_leaked,

    player_king_tower_hp,
    player_left_tower_hp,
    player_right_tower_hp,
    
    opp_king_tower_hp,
    opp_left_tower_hp,
    opp_right_tower_hp,
    
    -- calculated metrics

    case when player_crowns > opp_crowns then 1 else 0 end as is_victory,

    case when player_crowns = 3 then 1 else 0 end as is_three_crown_win,
    
    case when opp_left_tower_hp = 0 then 1 else 0 end as destroyed_left_tower,

    case when opp_right_tower_hp = 0 then 1 else 0 end as destroyed_right_tower,

    (
        (case when opp_left_tower_hp = 0 then 1 else 0 end) +
        (case when opp_right_tower_hp = 0 then 1 else 0 end) +
        (case when opp_king_tower_hp = 0 then 1 else 0 end)
    ) as total_towers_destroyed,

    (player_king_tower_hp + player_left_tower_hp + player_right_tower_hp) as player_total_remaining_hp,

    (opp_king_tower_hp + opp_left_tower_hp + opp_right_tower_hp) as opp_total_remaining_hp

from battles
  );
  