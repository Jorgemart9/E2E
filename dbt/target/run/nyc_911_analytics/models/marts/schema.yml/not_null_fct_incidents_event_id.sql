
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_id
from "new_york"."public_public"."fct_incidents"
where event_id is null



  
  
      
    ) dbt_internal_test