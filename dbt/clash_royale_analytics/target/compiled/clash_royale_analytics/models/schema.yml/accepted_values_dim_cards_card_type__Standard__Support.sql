
    
    

with all_values as (

    select
        card_type as value_field,
        count(*) as n_records

    from "cr_db"."public"."dim_cards"
    group by card_type

)

select *
from all_values
where value_field not in (
    'Standard','Support'
)


