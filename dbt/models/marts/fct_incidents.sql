with staging as (
    
    select * from {{ ref('stg_nyc_911_calls') }}
),

centroids as (

    select * from {{ ref('borough_centroids') }}

),

-- 1. Separamos los datos que YA tienen barrio
data_with_borough as (
    
    select * from staging 
    where borough is not null

),

-- 2. Separamos los datos que NO tienen barrio (NULL)
data_missing_borough as (
    
    select * from staging 
    where borough is null 
      and latitude is not null -- Solo podemos arreglar si tenemos coordenadas

),

-- 3. Magia: Cruzamos los nulos con los 5 centroides y calculamos la distancia
calculated_boroughs as (
    
    select 
        incident.event_id,
        centroid.borough_name as imputed_borough,
        -- Fórmula de distancia simple (Pitagoras) para encontrar el más cercano
        (power(incident.latitude - centroid.centroid_lat, 2) + 
         power(incident.longitude - centroid.centroid_lon, 2)) as distance_squared
    from data_missing_borough incident
    cross join centroids centroid

),

-- 4. Nos quedamos solo con el barrio más cercano para cada incidente
closest_match as (
    
    select distinct on (event_id)
        event_id,
        imputed_borough
    from calculated_boroughs
    order by event_id, distance_squared asc

),

-- 5. Unimos todo de nuevo
final_combined as (

    -- Filas originales que estaban bien
    select 
        event_id,
        incident_type_description,
        borough,
        dispatched_at, -- Necesarios para cálculo, luego los quitamos
        arrived_at,
        created_at,
        closed_at,
        latitude,
        longitude
    from data_with_borough

    union all

    -- Filas reparadas (unimos la data original con el barrio calculado)
    select 
        m.event_id,
        m.incident_type_description,
        c.imputed_borough as borough, -- Aquí usamos el barrio calculado
        m.dispatched_at,
        m.arrived_at,
        m.created_at,
        m.closed_at,
        m.latitude,
        m.longitude
    from data_missing_borough m
    join closest_match c on m.event_id = c.event_id

),

metrics as (

    select
        event_id,
        incident_type_description,
        borough,
        
        -- Cálculos (usamos las fechas aquí antes de descartarlas)
        extract(epoch from (arrived_at - dispatched_at)) / 60 as response_time_minutes,
        extract(epoch from (closed_at - created_at)) / 60 as total_duration_minutes,

        latitude,
        longitude

    from final_combined
    -- Filtros de limpieza
    where dispatched_at is not null 
      and arrived_at is not null

)

-- Selección final: Aquí EXCLUIMOS precinct_id, created_at y arrived_at como pediste
select 
    event_id,
    incident_type_description,
    borough,
    response_time_minutes,
    total_duration_minutes,
    latitude,
    longitude
from metrics