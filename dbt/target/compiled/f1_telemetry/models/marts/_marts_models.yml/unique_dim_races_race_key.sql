
    
    

select
    race_key as unique_field,
    count(*) as n_records

from "warehouse"."main_gold"."dim_races"
where race_key is not null
group by race_key
having count(*) > 1


