
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        rarity as value_field,
        count(*) as n_records

    from "cr_db"."public"."dim_cards"
    group by rarity

)

select *
from all_values
where value_field not in (
    'common','rare','epic','legendary','champion'
)



  
  
      
    ) dbt_internal_test