
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select record_id
from "warehouse"."main_gold"."fact_pit_stops"
where record_id is null



  
  
      
    ) dbt_internal_test