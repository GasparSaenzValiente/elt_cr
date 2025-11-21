
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select player_tag
from "cr_db"."public"."dim_players"
where player_tag is null



  
  
      
    ) dbt_internal_test