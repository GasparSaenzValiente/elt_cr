
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    clan_tag as unique_field,
    count(*) as n_records

from "cr_db"."public"."dim_clans"
where clan_tag is not null
group by clan_tag
having count(*) > 1



  
  
      
    ) dbt_internal_test