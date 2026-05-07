
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select race_year
from "warehouse"."main_gold"."dim_races"
where race_year is null



  
  
      
    ) dbt_internal_test