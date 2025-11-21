
    
    

select
    holding_key as unique_field,
    count(*) as n_records

from "cr_db"."public"."fct_player_card_holdings"
where holding_key is not null
group by holding_key
having count(*) > 1


