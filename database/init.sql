\c new_york;


CREATE TABLE 911_calls (
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
