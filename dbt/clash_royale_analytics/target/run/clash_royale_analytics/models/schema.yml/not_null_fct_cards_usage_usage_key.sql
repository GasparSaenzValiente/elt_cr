
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select usage_key
from "cr_db"."public"."fct_cards_usage"
where usage_key is null



  
  
      
    ) dbt_internal_test