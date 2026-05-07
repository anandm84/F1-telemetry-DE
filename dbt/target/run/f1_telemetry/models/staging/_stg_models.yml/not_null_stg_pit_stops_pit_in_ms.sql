
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pit_in_ms
from "warehouse"."main_silver"."stg_pit_stops"
where pit_in_ms is null



  
  
      
    ) dbt_internal_test