
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        played_by as value_field,
        count(*) as n_records

    from "cr_db"."public"."fct_cards_usage"
    group by played_by

)

select *
from all_values
where value_field not in (
    'Player','Opponent'
)



  
  
      
    ) dbt_internal_test