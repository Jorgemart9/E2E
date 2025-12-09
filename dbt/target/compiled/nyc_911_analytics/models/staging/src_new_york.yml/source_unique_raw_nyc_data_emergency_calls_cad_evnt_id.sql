
    
    

select
    cad_evnt_id as unique_field,
    count(*) as n_records

from "new_york"."public"."emergency_calls"
where cad_evnt_id is not null
group by cad_evnt_id
having count(*) > 1


