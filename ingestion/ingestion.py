import requests 
import time
import os 
import io
import pandas as pd

url_datos = "https://data.cityofnewyork.us/resource/n2zq-pubd.json"

url_api = os.getenv("API_URL", "http://api:5000") + "/ingest/911_calls"

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
                'cip_jobs': zip_jobs,
                'add_ts': add_ts,
                'disp_ts': disp_ts,
                'arrivd_ts': arrivd_ts,
                'closng_ts': closng_ts,
                'latitude': latitude,
                'longitude': longitude
            }
            lista_inserciones.append(fila)
        except ValueError:
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

