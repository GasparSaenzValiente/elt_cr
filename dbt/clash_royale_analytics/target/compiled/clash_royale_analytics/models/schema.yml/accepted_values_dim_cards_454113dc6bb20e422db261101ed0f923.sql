
    
    

with all_values as (

    select
        card_rarity as value_field,
        count(*) as n_records

    from "cr_db"."public"."dim_cards"
    group by card_rarity

)

select *
from all_values
where value_field not in (
    'common','rare','epic','legendary','champion'
)


