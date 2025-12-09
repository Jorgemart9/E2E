import requests 
import time
import os 
import io
import pandas as pd

url_datos = "https://data.cityofnewyork.us/resource/n2zq-pubd.json"

url_api = "http://api:5000/ingest/911_calls"

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
            cad_evnt_id = datos.get("cad_evnt_id")
            create_date = datos.get("create_date")
            incident_date = datos.get("incident_date")
            incident_time = datos.get("incident_time")
            nypd_pct_cd = datos.get("nypd_pct_cd")
            boro_nm = datos.get("boro_nm")
            patrl_boro_nm = datos.get("patrl_boro_nm")
            geo_cd_x = datos.get("geo_cd_x")
            geo_cd_y = datos.get("geo_cd_y")
            radio_code = datos.get("radio_code")
            typ_desc = datos.get("typ_desc")
            cip_jobs = datos.get("cip_jobs")
            add_ts = datos.get("add_ts")
            disp_ts = datos.get("disp_ts")
            arrivd_ts = datos.get("arrivd_ts")
            closng_ts = datos.get("closng_ts")
            latitude = datos.get("latitude")
            longitude = datos.get("longitude")
            fila = {
                'cad_envt_id': cad_evnt_id,
                'create_date': create_date,
                'incident_date': incident_date,
                'incident_time': incident_time,
                'nypd_pct_cd': nypd_pct_cd,
                'boro_nm': boro_nm,
                'patrl_boro_nm': patrl_boro_nm,
                'geo_cd_x': geo_cd_x,
                'geo_cd_y': geo_cd_y,
                'radio_code': radio_code,
                'typ_desc': typ_desc,
                'cip_jobs': cip_jobs,
                'add_ts': add_ts,
                'disp_ts': disp_ts,
                'arrivd_ts': arrivd_ts,
                'closng_ts': closng_ts,
                'latitude': latitude,
                'longitude': longitude
            }
            lista_inserciones.append(fila)
        except Exception as e:
            print(f"Error procesando fila: {e}")
            continue
    if lista_inserciones: 
        df = pd.DataFrame(lista_inserciones)
        columnas = [
            'cad_envt_id', 'create_date', 'incident_date', 'incident_time', 
            'nypd_pct_cd', 'boro_nm', 'patrl_boro_nm', 'geo_cd_x', 'geo_cd_y', 
            'radio_code', 'typ_desc', 'cip_jobs', 'add_ts', 'disp_ts', 
            'arrivd_ts', 'closng_ts', 'latitude', 'longitude'
        ]
        buffer = io.StringIO()
        df[columnas].to_csv(buffer, index=False, header=True, encoding='utf-8')
        buffer.seek(0)
        print(f"Enviando {len(df)} registros procesados a la API...")
        files = {'file': ('data.csv', buffer, 'text/csv')}
        try:
            res = requests.post(url_api, files=files, timeout=30)
            if res.status_code == 201:
                print("Datos insertados correctamente.")
                cont += 1
            else:
                print(f"Error API ({res.status_code}): {res.text}")
        except requests.exceptions.RequestException as e:
            print(f"Error enviando a API: {e}")
            time.sleep(600)
    else:
        print("No se generaron datos para insertar.")
    return cont
if __name__ == "__main__":
    print("Incluyendo datos en la API...")

    if cont is not None:
        cont = procesar_datos(cont)
        print("Procesamiento finalizado con éxito.")
    else:
        print("No se han descargado datos, saltando procesamiento.")
            
    print("--- Fin del script ---")

