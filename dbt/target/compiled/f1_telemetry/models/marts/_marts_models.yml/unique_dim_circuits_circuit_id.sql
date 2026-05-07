
    
    

select
    circuit_id as unique_field,
    count(*) as n_records

from "warehouse"."main_gold"."dim_circuits"
where circuit_id is not null
group by circuit_id
having count(*) > 1


