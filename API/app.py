from flask import Flask, request, jsonify
import psycopg
import os
import io
import time

app = Flask(__name__)

# Intentamos conectar al inicio (Bucle de reintento)
connection = None
MAX_RETRIES = 10
for i in range(MAX_RETRIES):
    try:
        # Asegúrate de que tu DATABASE_URL apunte a la BD 'new_york' (o la que uses)
        url = os.getenv("DATABASE_URL") 
        connection = psycopg.connect(url)
        print("BD conectada con éxito")
        break
    except Exception as e:
        print(f"Error conectando a la BD (intento {i+1}/{MAX_RETRIES}):", e)
        time.sleep(2)
else:
    print("FATAL: Falló la conexión a la base de datos después de varios intentos.")


@app.route('/ingest/911_calls', methods=['POST'])
def ingest_icad_events():
    # ... (El código de /ingest/911_calls se mantiene igual)
    # 1. Validaciones básicas del archivo
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if not connection:
        return jsonify({'error': 'No hay conexión a la base de datos'}), 500

    try:
        # 2. Leemos el archivo en memoria como texto
        stream = io.StringIO(file.stream.read().decode('utf-8'), newline=None)
        
        # 3. Definimos la consulta COPY con las columnas de icad_events
        sql = """
        COPY icad_events (
            cad_evnt_id, create_date, incident_date, incident_time, nypd_pct_cd, 
            boro_nm, patrl_boro_nm, geo_cd_x, geo_cd_y, radio_code, typ_desc, 
            cip_jobs, add_ts, disp_ts, arrivd_ts, closng_ts, latitude, longitude
        )
        FROM STDIN
        WITH (FORMAT CSV, HEADER)
        """
        
        # 4. Ejecutamos el copy dentro de un cursor transaccional
        with connection.cursor() as cur:
            with cur.copy(sql) as copy:
                copy.write(stream.getvalue())
            connection.commit()

        return jsonify({'message': 'Eventos ICAD ingestados correctamente'}), 201

    except Exception as e:
        # Hacemos rollback si algo falla
        if connection:
            connection.rollback()
        return jsonify({'error': str(e)}), 500

# -----------------------------------------------------------
# NUEVA RUTA PARA 311_CALLS
# -----------------------------------------------------------
@app.route('/ingest/311_calls', methods=['POST'])
def ingest_311_calls():
    """
    Ingesta masiva de datos de llamadas 311 de NYC en la tabla nyc_311_calls.
    """
    # 1. Validaciones básicas del archivo y la conexión
    if 'file' not in request.files:
        return jsonify({'error': 'No se encontró la parte del archivo (key "file")'}), 400
    
    if not connection:
        return jsonify({'error': 'No hay conexión a la base de datos'}), 500

    file = request.files['file']

    try:
        # 2. Leemos el archivo en memoria (debe estar en formato CSV)
        stream = io.StringIO(file.stream.read().decode('utf-8'), newline=None)
        
        # 3. Definimos la consulta COPY con TODAS las columnas.
        # Es crucial que el orden de las columnas coincida exactamente con el orden 
        # en que se genera el CSV en tu script de ingesta.
        sql = """
        COPY nyc_311_calls (
            unique_key, created_date, closed_date, agency, agency_name,
            complaint_type, descriptor, location_type, incident_zip, 
            incident_address, street_name, cross_street_1, cross_street_2,
            intersection_street_1, intersection_street_2, address_type, 
            city, landmark, facility_type, status, due_date,
            resolution_description, resolution_action_updated_date, 
            community_board, bbl, borough, x_coordinate_state_plane,
            y_coordinate_state_plane, open_data_channel_type, 
            park_facility_name, park_borough, vehicle_type,
            taxi_company_borough, taxi_pick_up_location, bridge_highway_name,
            bridge_highway_direction, road_ramp, bridge_highway_segment,
            latitude, longitude, location
        )
        FROM STDIN
        WITH (FORMAT CSV, HEADER)
        """
        
        # 4. Ejecutamos el copy dentro de un cursor transaccional
        with connection.cursor() as cur:
            with cur.copy(sql) as copy:
                copy.write(stream.getvalue())
            connection.commit()

        # 5. Respuesta exitosa
        return jsonify({'message': f'Ingesta de {cur.rowcount} llamadas 311 completada con éxito'}), 201

    except Exception as e:
        # 6. Manejo de errores y rollback
        if connection:
            connection.rollback()
        return jsonify({'error': f'Error durante la ingesta de 311: {str(e)}'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)