
    
    

select
    usage_key as unique_field,
    count(*) as n_records

from "cr_db"."public"."fct_cards_usage"
where usage_key is not null
group by usage_key
having count(*) > 1


