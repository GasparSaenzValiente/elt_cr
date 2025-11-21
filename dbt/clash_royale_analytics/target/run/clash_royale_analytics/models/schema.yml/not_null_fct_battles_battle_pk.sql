
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select battle_pk
from "cr_db"."public"."fct_battles"
where battle_pk is null



  
  
      
    ) dbt_internal_test