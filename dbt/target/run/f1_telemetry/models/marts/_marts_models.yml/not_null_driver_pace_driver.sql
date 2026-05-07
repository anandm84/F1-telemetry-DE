
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select driver
from "warehouse"."main_gold"."driver_pace"
where driver is null



  
  
      
    ) dbt_internal_test