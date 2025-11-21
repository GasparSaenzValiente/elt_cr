
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select card_id as from_field
    from "cr_db"."public"."fct_player_card_holdings"
    where card_id is not null
),

parent as (
    select card_id as to_field
    from "cr_db"."public"."dim_cards"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test