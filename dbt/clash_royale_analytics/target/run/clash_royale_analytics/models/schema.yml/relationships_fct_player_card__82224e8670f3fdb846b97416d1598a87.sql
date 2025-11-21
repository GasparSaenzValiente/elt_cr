
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select player_tag as from_field
    from "cr_db"."public"."fct_player_card_holdings"
    where player_tag is not null
),

parent as (
    select player_tag as to_field
    from "cr_db"."public"."dim_players"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test