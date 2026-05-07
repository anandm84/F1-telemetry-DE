
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    record_id as unique_field,
    count(*) as n_records

from "warehouse"."main_gold"."fact_pit_stops"
where record_id is not null
group by record_id
having count(*) > 1



  
  
      
    ) dbt_internal_test