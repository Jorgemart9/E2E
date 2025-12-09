
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select response_time_minutes
from "new_york"."public_public"."fct_incidents"
where response_time_minutes is null



  
  
      
    ) dbt_internal_test