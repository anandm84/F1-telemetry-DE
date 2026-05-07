
    
    

select
    record_id as unique_field,
    count(*) as n_records

from "warehouse"."main_gold"."fact_race_results"
where record_id is not null
group by record_id
having count(*) > 1


