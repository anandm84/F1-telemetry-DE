
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lap_number
from "warehouse"."main_silver"."stg_laps"
where lap_number is null



  
  
      
    ) dbt_internal_test