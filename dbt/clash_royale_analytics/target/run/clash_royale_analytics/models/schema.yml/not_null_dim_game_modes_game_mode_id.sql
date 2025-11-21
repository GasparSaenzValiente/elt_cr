
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select game_mode_id
from "cr_db"."public"."dim_game_modes"
where game_mode_id is null



  
  
      
    ) dbt_internal_test