import requests 
import time
import os 
import io
import pandas as pd

url_datos = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

url_api = os.getenv("API_URL", "http://api:5000") + "/ingest/311_calls"

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
            unique_key = datos.get("unique_key")
            created_date = datos.get("created_date")
            closed_date = datos.get("closed_date")
            agency = datos.get("agency")
            agency_name = datos.get("agency_name")
            complaint_type = datos.get("complaint_type")
            descriptor = datos.get("descriptor")
            location_type = datos.get("location_type")
            incident_zip = datos.get("incident_zip")
            incident_address = datos.get("incident_address")
            street_name = datos.get("street_name")
            cross_street_1 = datos.get("cross_street_1")
            cross_street_2 = datos.get("cross_street_2")
            intersection_street_1 = datos.get("intersection_street_1")
            intersection_street_2 = datos.get("intersection_street_2")
            address_type = datos.get("address_type")
            city = datos.get("city")
            landmark = datos.get("landmark")
            facility_type = datos.get("facility_type")
            status = datos.get("status")
            due_date = datos.get("due_date")
            resolution_description = datos.get("resolution_description")
            resolution_action_updated_date = datos.get("resolution_action_updated_date")
            community_board = datos.get("community_board")
            bbl = datos.get("bbl")
            borough = datos.get("borough")
            x_coordinate_state_plane = datos.get("x_coordinate_state_plane")
            y_coordinate_state_plane = datos.get("y_coordinate_state_plane")
            open_data_channel_type = datos.get("open_data_channel_type")
            park_facility_name = datos.get("park_facility_name")
            park_borough = datos.get("park_borough")
            vehicle_type = datos.get("vehicle_type")
            taxi_company_borough = datos.get("taxi_company_borough")
            taxi_pick_up_location = datos.get("taxi_pick_up_location")
            bridge_highway_name = datos.get("bridge_highway_name")
            bridge_highway_direction = datos.get("bridge_highway_direction")
            road_ramp = datos.get("road_ramp")
            bridge_highway_segment = datos.get("bridge_highway_segment")
            latitude = datos.get("latitude")
            longitude = datos.get("longitude")
            location = datos.get("location")
            
            fila = {
                'unique_key': unique_key,
                'created_date': created_date,
                'closed_date': closed_date,
                'agency': agency,
                'agency_name': agency_name,
                'complaint_type': complaint_type,
                'descriptor': descriptor,
                'location_type': location_type,
                'incident_zip': incident_zip,
                'incident_address': incident_address,
                'street_name': street_name,
                'cross_street_1': cross_street_1,
                'cross_street_2': cross_street_2,
                'intersection_street_1': intersection_street_1,
                'intersection_street_2': intersection_street_2,
                'address_type': address_type,
                'city': city,
                'landmark': landmark,
                'facility_type': facility_type,
                'status': status,
                'due_date': due_date,
                'resolution_description': resolution_description,
                'resolution_action_updated_date': resolution_action_updated_date,
                'community_board': community_board,
                'bbl': bbl,
                'borough': borough,
                'x_coordinate_state_plane': x_coordinate_state_plane,
                'y_coordinate_state_plane': y_coordinate_state_plane,
                'open_data_channel_type': open_data_channel_type,
                'park_facility_name': park_facility_name,
                'park_borough': park_borough,
                'vehicle_type': vehicle_type,
                'taxi_company_borough': taxi_company_borough,
                'taxi_pick_up_location': taxi_pick_up_location,
                'bridge_highway_name': bridge_highway_name,
                'bridge_highway_direction': bridge_highway_direction,
                'road_ramp': road_ramp,
                'bridge_highway_segment': bridge_highway_segment,
                'latitude': latitude,
                'longitude': longitude,
                'location': str(location) if location else None
            }
            lista_inserciones.append(fila)
        except Exception as e:
            print(f"Error procesando fila: {e}")
            continue
    if lista_inserciones: 
        df = pd.DataFrame(lista_inserciones)
        columnas = [
            'unique_key', 'created_date', 'closed_date', 'agency', 'agency_name',
            'complaint_type', 'descriptor', 'location_type', 'incident_zip', 
            'incident_address', 'street_name', 'cross_street_1', 'cross_street_2',
            'intersection_street_1', 'intersection_street_2', 'address_type', 
            'city', 'landmark', 'facility_type', 'status', 'due_date',
            'resolution_description', 'resolution_action_updated_date', 
            'community_board', 'bbl', 'borough', 'x_coordinate_state_plane',
            'y_coordinate_state_plane', 'open_data_channel_type', 
            'park_facility_name', 'park_borough', 'vehicle_type',
            'taxi_company_borough', 'taxi_pick_up_location', 'bridge_highway_name',
            'bridge_highway_direction', 'road_ramp', 'bridge_highway_segment',
            'latitude', 'longitude', 'location'
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