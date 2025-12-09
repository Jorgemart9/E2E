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
