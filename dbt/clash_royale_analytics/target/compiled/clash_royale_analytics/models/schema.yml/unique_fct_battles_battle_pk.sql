
    
    

select
    battle_pk as unique_field,
    count(*) as n_records

from "cr_db"."public"."fct_battles"
where battle_pk is not null
group by battle_pk
having count(*) > 1


