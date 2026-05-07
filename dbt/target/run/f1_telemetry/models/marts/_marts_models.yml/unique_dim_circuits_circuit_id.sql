
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    circuit_id as unique_field,
    count(*) as n_records

from "warehouse"."main_gold"."dim_circuits"
where circuit_id is not null
group by circuit_id
having count(*) > 1



  
  
      
    ) dbt_internal_test