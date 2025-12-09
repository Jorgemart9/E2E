
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cad_evnt_id
from "new_york"."public"."emergency_calls"
where cad_evnt_id is null



  
  
      
    ) dbt_internal_test