
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    game_mode_id as unique_field,
    count(*) as n_records

from "cr_db"."public"."dim_game_modes"
where game_mode_id is not null
group by game_mode_id
having count(*) > 1



  
  
      
    ) dbt_internal_test