
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    player_tag as unique_field,
    count(*) as n_records

from "cr_db"."public"."dim_players"
where player_tag is not null
group by player_tag
having count(*) > 1



  
  
      
    ) dbt_internal_test