
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lap_time_ms
from "warehouse"."main_silver"."stg_laps"
where lap_time_ms is null



  
  
      
    ) dbt_internal_test