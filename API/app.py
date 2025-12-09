from flask import Flask, request, jsonify
import psycopg
import os
import io
import time

app = Flask(__name__)

for i in range(10):
    try:
        #URL CONEXIÓN A BD 
        url = os.getenv("DATABASE_URL")
        #CONEXIÓN A BD
        connection = psycopg.connect(url)
        # Cursor
        cur = connection.cursor()
        print("BD conectada con éxito")
        break
    except Exception as e :
        print("Error conectando a la BD:", e)
        time.sleep(2)

@app.route('/ingest/calidad_aire', methods=['POST'])
def ingest_data():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    try:
        stream = io.StringIO(file.stream.read().decode('utf-8'), newline=None)
        
        sql = """
        COPY calidad_aire (
            MUNICIPIO, 
            ESTACION, 
            MAGNITUD, 
            PUNTO_MUESTREO, 
            ANO, 
            MES, 
            DIA, 
            HORA, 
            VALOR, 
            VALIDACION,
            LAT, 
            LON
        )
        FROM STDIN
        WITH (FORMAT CSV)
        """
        with cur.copy(sql) as copy:
            copy.write(stream.getvalue())
        connection.commit()

        return jsonify({'message': 'Datos ingestados correctamente'}), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)