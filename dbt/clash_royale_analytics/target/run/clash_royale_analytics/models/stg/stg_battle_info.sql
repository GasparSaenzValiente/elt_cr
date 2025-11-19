
  create view "cr_db"."public"."stg_battle_info__dbt_tmp"
    
    
  as (
    with source_data as (
    select * from "cr_db"."public"."landing_battle_info"
)
select distinct on (battle_id, player_tag)
    cast(battle_id as varchar(64)) as battle_id,
    cast(battle_time as timestamp) as battle_time,
    game_mode_id,
    cast(game_mode_name as varchar(50)) as game_mode_name,
    is_ladder_tournament,
    is_hosted_match,
    
    cast(player_tag as varchar(9)) as player_tag,
    cast(player_elixir_leaked as smallint) as player_elixir_leaked,
    cast(player_king_tower_hit_points as smallint) as player_king_tower_hp,
    cast(player_princess_tower_hit_points[1] as smallint) as player_left_tower_hp,
    cast(player_princess_tower_hit_points[2] as smallint) as player_right_tower_hp,
    cast(player_trophy_change as smallint) as player_trophy_change,
    cast(player_crowns as smallint) as player_crowns,

    cast(opp_tag as varchar(9)) as opp_tag,
    cast(opp_elixir_leaked as smallint) as opp_elixir_leaked,
    cast(opp_king_tower_hit_points as smallint) as opp_king_tower_hp,
    cast(opp_princess_tower_hit_points[1] as smallint) as opp_left_tower_hp,
    cast(opp_princess_tower_hit_points[2] as smallint) as opp_right_tower_hp,
    cast(opp_trophy_change as smallint) as opp_trophy_change,
    cast(opp_crowns as smallint) as opp_crowns,
    cast(snapshot_date as date) as snapshot_date
from source_data
order by battle_id, player_tag
  );