
    
    

with all_values as (

    select
        player_crowns as value_field,
        count(*) as n_records

    from "cr_db"."public"."fct_battles"
    group by player_crowns

)

select *
from all_values
where value_field not in (
    '0','1','2','3'
)


