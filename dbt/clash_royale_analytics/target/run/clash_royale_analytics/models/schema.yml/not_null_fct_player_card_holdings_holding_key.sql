
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select holding_key
from "cr_db"."public"."fct_player_card_holdings"
where holding_key is null



  
  
      
    ) dbt_internal_test