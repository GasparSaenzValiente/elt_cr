
    
    

select
    clan_tag as unique_field,
    count(*) as n_records

from "cr_db"."public"."dim_clans"
where clan_tag is not null
group by clan_tag
having count(*) > 1


