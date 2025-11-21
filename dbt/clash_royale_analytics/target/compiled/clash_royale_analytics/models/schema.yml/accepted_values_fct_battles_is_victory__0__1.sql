
    
    

with all_values as (

    select
        is_victory as value_field,
        count(*) as n_records

    from "cr_db"."public"."fct_battles"
    group by is_victory

)

select *
from all_values
where value_field not in (
    '0','1'
)


