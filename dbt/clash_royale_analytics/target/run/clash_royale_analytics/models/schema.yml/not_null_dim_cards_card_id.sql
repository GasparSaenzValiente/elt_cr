
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select card_id
from "cr_db"."public"."dim_cards"
where card_id is null



  
  
      
    ) dbt_internal_test