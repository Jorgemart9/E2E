from flask import Flask, request, jsonify
import psycopg
import os
import io
import time

app = Flask(__name__)

# Intentamos conectar al inicio (Bucle de reintento)
connection = None
for i in range(10):
    try:
        # Asegúrate de que tu DATABASE_URL apunte a la BD 'new_york'
        url = os.getenv("DATABASE_URL") 
        connection = psycopg.connect(url)
        print("BD 'new_york' conectada con éxito")
        break
    except Exception as e:
        print(f"Error conectando a la BD (intento {i+1}):", e)
        time.sleep(2)

@app.route('/ingest/911_calls', methods=['POST'])
def ingest_icad_events():
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
        COPY emergency_calls (
            cad_evnt_id,
            create_date,
            incident_date,
            incident_time,
            nypd_pct_cd,
            boro_nm,
            patrl_boro_nm,
            geo_cd_x,
            geo_cd_y,
            radio_code,
            typ_desc,
            cip_jobs,
            add_ts,
            disp_ts,
            arrivd_ts,
            closng_ts,
            latitude,
            longitude
        )
        FROM STDIN
        WITH (FORMAT CSV, HEADER)
        """
        
        # NOTA: He añadido 'HEADER' en el WITH por si tu CSV trae la fila de títulos.
        # Si el CSV NO tiene cabecera, borra ", HEADER".

        # 4. Ejecutamos el copy dentro de un cursor transaccional
        with connection.cursor() as cur:
            with cur.copy(sql) as copy:
                copy.write(stream.getvalue())
            connection.commit()

        return jsonify({'message': 'Eventos ICAD ingestados correctamente'}), 201

    except Exception as e:
        # Hacemos rollback si algo falla para no dejar la transacción abierta
        if connection:
            connection.rollback()
        return jsonify({'error': "Something happened with the database"}), 500

@app.route('/emergency_calls', methods=['GET'])
def get_emergency_calls():
    if not connection:
        return jsonify({'error': 'No hay conexión a la base de datos'}), 500

    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT 
                    ec.*,
                    cc.latitude AS central_latitude,
                    cc.longitude AS central_longitude
                FROM emergency_calls ec
                LEFT JOIN central_coordinates cc
                    ON ec.boro_nm = cc.boro_nm
            """)
            
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            results = [dict(zip(colnames, row)) for row in rows]

        return jsonify(results), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)