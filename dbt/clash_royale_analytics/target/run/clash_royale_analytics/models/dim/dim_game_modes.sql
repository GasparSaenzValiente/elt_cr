
  
    

  create  table "cr_db"."public"."dim_game_modes__dbt_tmp"
  
  
    as
  
  (
    select distinct
    game_mode_id,
    game_mode_name
from "cr_db"."public"."stg_battle_info"
  );
  