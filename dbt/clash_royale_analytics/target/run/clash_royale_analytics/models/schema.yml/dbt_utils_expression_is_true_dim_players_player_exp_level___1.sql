
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "cr_db"."public"."dim_players"

where not(player_exp_level >= 1)


  
  
      
    ) dbt_internal_test