import requests 
import time
import os 
import io
import pandas as pd

url_datos = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

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
            