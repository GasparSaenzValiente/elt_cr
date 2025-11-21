
    
    

select
    player_tag as unique_field,
    count(*) as n_records

from "cr_db"."public"."dim_players"
where player_tag is not null
group by player_tag
having count(*) > 1


