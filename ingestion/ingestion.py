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




    print(f"Transformando {len(data)} registros (Wide -> Long)...")
    
    lista_inserciones = []
    
    for datos in data:
        try:
            municipio = int(datos['MUNICIPIO'])
            estacion = datos['ESTACION']
            magnitud = datos['MAGNITUD']
            punto = datos['PUNTO_MUESTREO']
            ano = int(datos['ANO'])
            mes = int(datos['MES'])
            dia = int(datos['DIA'])

            for hora in range(1, 25):
                clave_valor = f"H{hora:02d}"
                clave_validacion = f"V{hora:02d}"

                if clave_valor in datos:
                    valor_hora = float(datos[clave_valor])
                    validacion_hora = datos[clave_validacion]

                    fila = {
                        'MUNICIPIO': municipio,
                        'ESTACION': estacion,
                        'MAGNITUD': magnitud,
                        'PUNTO_MUESTREO': punto,
                        'ANO': ano,
                        'MES': mes,
                        'DIA': dia,
                        'HORA': hora,
                        'VALOR': valor_hora,
                        'VALIDACION': validacion_hora,
                        'LAT': 0.0,
                        'LON': 0.0
                    }
                    lista_inserciones.append(fila)
        except ValueError:
            continue

    if lista_inserciones:
        df = pd.DataFrame(lista_inserciones)
        df = df[(df['VALIDACION'] != 'N')]
        if cont == 0:
            buffer = io.StringIO()

            columnas = ['MUNICIPIO', 'ESTACION', 'MAGNITUD', 'PUNTO_MUESTREO', 
                        'ANO', 'MES', 'DIA', 'HORA', 'VALOR', 'VALIDACION', 'LAT', 'LON']
            
            df[columnas].to_csv(buffer, index=False, header=False, encoding = 'utf-8')
            buffer.seek(0)

            print(f"Enviando {len(df)} filas procesadas a la API...")
            files = {'file': ('data.csv', buffer)}
            res = requests.post(INTERNAL_API_URL, files=files)

            if res.status_code == 201:
                print("Datos insertados correctamente.")
                cont += 1
            else:
                print(f"Error API: {res.text}")
        else:
            columnas_agrupacion = ['ESTACION', 'MAGNITUD', 'ANO', 'MES', 'DIA']
            indices_ultima_hora = df.groupby(columnas_agrupacion)['HORA'].idxmax()
            df = df.loc[indices_ultima_hora]
            print(len(df))
            buffer = io.StringIO()

            columnas = ['MUNICIPIO', 'ESTACION', 'MAGNITUD', 'PUNTO_MUESTREO', 
                        'ANO', 'MES', 'DIA', 'HORA', 'VALOR', 'VALIDACION', 'LAT', 'LON']
            
            df[columnas].to_csv(buffer, index=False, header=False, encoding = 'utf-8')
            buffer.seek(0)

            print(f"Enviando {len(df)} filas procesadas a la API de Madrid...")
            files = {'file': ('data.csv', buffer)}
            res = requests.post(INTERNAL_API_URL, files=files)

            if res.status_code == 201:
                print("Datos insertados correctamente de Madrid.")
                cont += 1
            else:
                print(f"Error API: {res.text}")
    else:
        print("No se generaron datos para insertar.")
    return cont

if __name__ == "__main__":
    print("--- Iniciando ejecución orquestada por Airflow ---")

    if cont is not None:
        cont = procesar_datos(cont)
        print("Procesamiento finalizado con éxito.")
    else:
        print("[ERROR] No se han descargado datos, saltando procesamiento.", flush=True)
            
    print("--- Fin del script ---")