
    
    

with child as (
    select card_id as from_field
    from "cr_db"."public"."fct_cards_usage"
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


