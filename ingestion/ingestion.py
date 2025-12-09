import requests 
import time
import os 
import io

url_datos = "https://data.cityofnewyork.us/resource/n2zq-pubd.json"

url_api = ""

cont = 0
def procesar_datos(cont):
    print("Descargando datos...")
    try:
        resp = requests.get(url_datos)
        data = resp.json()
    except Exception as e:
        print(f"Error descargando: {e}")
        return
    lista_inserciones = []
    for datos in data:
        try:
            cad_evnt_id = datos["cad_evnt_id"]
            create_date = datos["create_date"]
            incident_date = datos["incident_date"]
            incident_time = datos["incident_time"]
            nypd_pct_cd = datos["nypd_pct_cd"]
            boro_nm = datos["boro_nm"]
            patrl_boro_nm = datos["patrl_boro_nm"]
            geo_cd_x = datos["geo_cd_x"]
            geo_cd_y = datos["geo_cd_y"]
            radio_code = datos["radio_code"]
            typ_desc = datos["typ_desc"]
            zip_jobs = datos["zip_jobs"]
            add_ts = datos["add_ts"]
            disp_ts = datos["disp_ts"]
            arrivd_ts = datos["arrivd_ts"]
            closng_ts = datos["closng_ts"]
            latitude = datos["latitude"]
            longitude = datos["longitude"]

            fila = {
                'CAD_EVNT_ID': cad_evnt_id,
                'CREATE_DATE': create_date,
                'INCIDENT_DATE': incident_date,
                'INCIDENT_TIME': incident_time,
                'NYPD_PCT_CD': nypd_pct_cd,
                'BORO_NM': boro_nm,
                'PATRL_BORO_NM': patrl_boro_nm,
                'GEO_CD_X': geo_cd_x,
                'GEO_CD_Y': geo_cd_y,
                'RADIO_CODE': radio_code,
                'TYPE_DESC': typ_desc,
                'ZIP_JOBS': zip_jobs,
                'ADD_TS': add_ts,
                'DISP_TS': disp_ts,
                'ARRIVD_TS': arrivd_ts,
                'CLOSNG_TS': closng_ts,
                'LATITUDE': latitude,
                'LONGITUDE': longitude
            }
            lista_inserciones.append(fila)
    except Exception as e:
        print(f"Error procesando datos: {e}")

