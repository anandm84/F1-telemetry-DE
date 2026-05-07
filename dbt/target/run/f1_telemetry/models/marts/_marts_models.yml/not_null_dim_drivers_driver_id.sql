
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select driver_id
from "warehouse"."main_gold"."dim_drivers"
where driver_id is null



  
  
      
    ) dbt_internal_test