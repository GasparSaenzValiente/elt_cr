
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select game_mode_id as from_field
    from "cr_db"."public"."fct_battles"
    where game_mode_id is not null
),

parent as (
    select game_mode_id as to_field
    from "cr_db"."public"."dim_game_modes"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test