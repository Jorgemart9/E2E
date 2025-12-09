with source as (

    select * from "new_york"."public"."emergency_calls"

),

renamed as (

    select
        -- IDs y Claves
        cad_evnt_id as event_id,
        
        -- Fechas y Tiempos
        create_date as created_at,
        incident_date,
        incident_time, -- Podrías intentar combinarlos si incident_date no tiene hora
        add_ts as added_at,
        disp_ts as dispatched_at,
        arrivd_ts as arrived_at,
        closng_ts as closed_at,

        -- Detalles del Incidente
        typ_desc as incident_type_description,
        radio_code,
        cip_jobs as job_status, -- Critical Injury / Police jobs
        
        -- Ubicación
        boro_nm as borough,
        patrl_boro_nm as patrol_borough,
        nypd_pct_cd as precinct_id,
        latitude,
        longitude,
        geo_cd_x,
        geo_cd_y

    from source

)

select * from renamed