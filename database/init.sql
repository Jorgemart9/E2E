\c new_york;


CREATE TABLE IF NOT EXISTS emergency_calls (
    cad_evnt_id        TEXT PRIMARY KEY,
    create_date        TIMESTAMP,
    incident_date      TIMESTAMP,
    incident_time      TEXT,
    nypd_pct_cd        INTEGER,
    boro_nm            TEXT,
    patrl_boro_nm      TEXT,
    geo_cd_x           TEXT,
    geo_cd_y           TEXT,
    radio_code         TEXT,
    typ_desc           TEXT,
    cip_jobs           TEXT,
    add_ts             TIMESTAMP,
    disp_ts            TIMESTAMP,
    arrivd_ts          TIMESTAMP,
    closng_ts          TIMESTAMP,
    latitude           DOUBLE PRECISION,
    longitude          DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS nyc_311_calls (
    -- Clave Única y Fechas
    unique_key NUMERIC PRIMARY KEY, 
    created_date TIMESTAMP WITHOUT TIME ZONE,
    closed_date TIMESTAMP WITHOUT TIME ZONE,
    due_date TIMESTAMP WITHOUT TIME ZONE,
    resolution_action_updated_date TIMESTAMP WITHOUT TIME ZONE,

    -- Agencias y Tipos de Queja
    agency VARCHAR(50),
    agency_name VARCHAR(255),
    complaint_type VARCHAR(255),
    descriptor VARCHAR(255),
    
    -- Ubicación y Direcciones
    location_type VARCHAR(255),
    incident_zip VARCHAR(10), -- Mejor VARCHAR, ya que no se usa para cálculos
    incident_address VARCHAR(255),
    street_name VARCHAR(255),
    cross_street_1 VARCHAR(255),
    cross_street_2 VARCHAR(255),
    intersection_street_1 VARCHAR(255),
    intersection_street_2 VARCHAR(255),
    address_type VARCHAR(50),
    city VARCHAR(100),
    borough VARCHAR(50),
    community_board VARCHAR(50), 
    bbl NUMERIC,
    landmark VARCHAR(255),
    facility_type VARCHAR(255),

    -- Estado y Resolución
    status VARCHAR(50),
    resolution_description TEXT,

    -- Coordenadas y Geografía
    x_coordinate_state_plane NUMERIC,
    y_coordinate_state_plane NUMERIC,
    latitude NUMERIC,
    longitude NUMERIC,
    location TEXT, -- Se usa TEXT para almacenar el objeto JSON/Diccionario completo de 'location'

    -- Canales y Tipos Específicos
    open_data_channel_type VARCHAR(50),
    park_facility_name VARCHAR(255),
    park_borough VARCHAR(50),
    vehicle_type VARCHAR(100),
    taxi_company_borough VARCHAR(50),
    taxi_pick_up_location VARCHAR(255),
    bridge_highway_name VARCHAR(255),
    bridge_highway_direction VARCHAR(100),
    road_ramp VARCHAR(50),
    bridge_highway_segment VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS central_coordinates (
    boro_nm        TEXT PRIMARY KEY,
    latitude       DOUBLE PRECISION,
    longitude      DOUBLE PRECISION
);

INSERT INTO central_coordinates (boro_nm, latitude, longitude) VALUES
('BRONX', 40.8448, -73.8648),
('BROOKLYN', 40.6782, -73.9442),
('MANHATTAN', 40.7831, -73.9712),
('QUEENS', 40.7282, -73.7949),
('STATEN ISLAND', 40.5795, -74.1502);