
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select battle_key
from "cr_db"."public"."fct_battles"
where battle_key is null



  
  
      
    ) dbt_internal_test