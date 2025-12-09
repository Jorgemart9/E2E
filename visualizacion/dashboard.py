import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
import numpy as np

# 1. Cargar y Limpiar datos
try:
    df = pd.read_csv('NYPD_Calls_for_Service_(Year_to_Date)_20251209-2.csv')
except FileNotFoundError:
    # Datos dummy para prueba
    data = {
        'BORO_NM': ['MANHATTAN', 'QUEENS', 'BROOKLYN', 'BRONX', 'STATEN ISLAND', None] * 20,
        'TYP_DESC': ['ROBBERY', 'BURGLARY', 'ASSAULT', 'LARCENY', 'DRUGS', 'ROBBERY'] * 20,
        'ARRIVD_TS': ['2025-01-01 12:00:00'] * 120,
        'DISP_TS': ['2025-01-01 11:50:00'] * 120,
        'CLOSNG_TS': ['2025-01-01 12:30:00'] * 120
    }
    df = pd.DataFrame(data)

# Limpieza básica
df = df.dropna(subset=['BORO_NM'])
df['BORO_NM'] = df['BORO_NM'].astype(str).str.upper().str.strip()

# --- Calcular tiempos medios por borough ---
def calcular_tiempos(df):
    # Convertir a datetime
    df = df.copy()
    date_format = "%Y-%m-%d %H:%M:%S"
    for col in ['ARRIVD_TS', 'DISP_TS', 'CLOSNG_TS']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format=date_format, errors='coerce')
    # Tiempo de llegada: ARRIVD_TS - DISP_TS
    df['tiempo_llegada'] = (df['ARRIVD_TS'] - df['DISP_TS']).dt.total_seconds() / 60
    # Tiempo de duración: CLOSNG_TS - ARRIVD_TS
    df['tiempo_duracion'] = (df['CLOSNG_TS'] - df['ARRIVD_TS']).dt.total_seconds() / 60
    # Agrupar por borough
    tabla = df.groupby('BORO_NM').agg({
        'tiempo_llegada': 'mean',
        'tiempo_duracion': 'mean'
    }).reset_index()
    tabla = tabla.round({'tiempo_llegada': 2, 'tiempo_duracion': 2})
    return tabla

tabla_tiempos = calcular_tiempos(df)

# 2. Datos Geográficos y Población
info_distritos = {
    'BORO_NM': ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND'],
    'Latitud': [40.8448, 40.6782, 40.7831, 40.7282, 40.5795],
    'Longitud': [-73.8648, -73.9442, -73.9712, -73.7949, -74.1502],
    'Poblacion': [1356476, 2561225, 1597451, 2252196, 490687]
}
df_info = pd.DataFrame(info_distritos)

# 3. Agrupación y Cálculos
df_agrupado = df.groupby(['BORO_NM', 'TYP_DESC']).size().reset_index(name='count')
df_final = pd.merge(df_agrupado, df_info, on='BORO_NM', how='left')
df_final = df_final.dropna(subset=['Poblacion'])

# 4. Preparar Datos para "TODOS" (Índice / Tasa)
df_total_boro = df_final.groupby('BORO_NM').agg({
    'count': 'sum', 
    'Poblacion': 'first',
    'Latitud': 'first', 
    'Longitud': 'first'
}).reset_index()

# Calculamos el índice (Tasa por 1,000 habitantes)
df_total_boro['tasa_total'] = (df_total_boro['count'] / df_total_boro['Poblacion']) * 1000
df_total_boro['tasa_total'] = df_total_boro['tasa_total'].fillna(0)

fig = go.Figure()

# --- TRACE 0: VISTA "TODOS" (Usa Índice/Tasa) ---
fig.add_trace(go.Scattermapbox(
    lat=df_total_boro['Latitud'],
    lon=df_total_boro['Longitud'],
    mode='markers+text',
    marker=dict(
        # El tamaño depende del ÍNDICE (Tasa)
        size=df_total_boro['tasa_total'] * 2, 
        color=df_total_boro['tasa_total'],
        colorscale='RdYlGn_r',
        showscale=False, # <-- AQUI QUITAMOS EL GRADIENTE DE LA DERECHA
        opacity=0.8,
    ),
    text=df_total_boro['BORO_NM'],
    textposition='top center',
    hovertemplate='<b>%{text}</b><br>Índice Criminalidad: %{marker.color:.2f} (por 1k hab)<br>Total Absoluto: %{customdata}<extra></extra>',
    customdata=df_total_boro['count'],
    name='Índice General'
))

# --- TRACES 1..N: TIPOS ESPECÍFICOS (Usa Count Total) ---
tipos_delito = sorted(df_final['TYP_DESC'].unique())

# Para normalizar el tamaño de las burbujas de conteo (que son números grandes)
max_count_global = df_final['count'].max()

for tipo_delito in tipos_delito:
    df_tipo = df_final[df_final['TYP_DESC'] == tipo_delito]
    
    if df_tipo.empty:
        continue

    fig.add_trace(go.Scattermapbox(
        lat=df_tipo['Latitud'],
        lon=df_tipo['Longitud'],
        mode='markers+text',
        marker=dict(
            # El tamaño depende del CONTEO TOTAL (Normalizado para que no sea gigante)
            size=(df_tipo['count'] / max_count_global) * 80 + 20, 
            color=df_tipo['count'], # El color también refleja cantidad
            colorscale='Reds',
            showscale=False, # Sin barra de gradiente
            opacity=0.7
        ),
        text=df_tipo['BORO_NM'],
        textposition='top center',
        # Hover muestra el total numérico
        hovertemplate='<b>%{text}</b><br>Total Delitos: %{customdata}<extra></extra>',
        customdata=df_tipo['count'],
        name=tipo_delito,
        visible=False
    ))

# Configuración del Mapa
fig.update_layout(
    mapbox=dict(
        style='carto-positron',
        zoom=9.5,
        center=dict(lat=40.7128, lon=-74.0060)
    ),
    margin=dict(r=0, t=80, l=0, b=0),
    title="Mapa de Criminalidad",
    showlegend=False
)

# --- BOTONES ---
buttons = []
num_traces = len(fig.data)

# Botón 1: TODOS
buttons.append(dict(
    label="Todos (Índice)",
    method="update",
    args=[
        {"visible": [True] + [False] * (num_traces - 1)}, 
        {"title": "Índice de Criminalidad (Tasa por Habitante)"}
    ]
))

# Botones 2..N: Tipos Específicos
for i in range(1, num_traces):
    trace_name = fig.data[i].name
    visibilidad = [False] * num_traces
    visibilidad[i] = True
    
    buttons.append(dict(
        label=trace_name,
        method="update",
        args=[
            {"visible": visibilidad},
            {"title": f"Total de Delitos: {trace_name} (Cantidad Absoluta)"}
        ]
    ))

fig.update_layout(
    updatemenus=[
        dict(
            buttons=buttons,
            direction="down",
            pad={"r": 10, "t": 10},
            showactive=True,
            x=0.01,
            xanchor="left",
            y=0.99,
            yanchor="top",
            bgcolor="white"
        ),
    ]
)

app = Dash(__name__)

from dash.dependencies import Input, Output, State

app.layout = html.Div([
    html.H1("Análisis de Criminalidad NYPD", style={"textAlign": "center", "fontFamily": "Arial"}),
    html.Div([
        html.Div([
            dcc.Graph(id="mapa-crimen", figure=fig, style={"height": "90vh", "width": "100%"})
        ], style={"width": "66%", "display": "inline-block", "verticalAlign": "top"}),
        html.Div([
            html.Div(id="tabla-tiempos", style={"display": "none"})
        ], style={"width": "33%", "display": "inline-block", "verticalAlign": "top", "paddingLeft": "20px"})
    ], style={"width": "100%", "display": "flex"}),
    dcc.Store(id="dropdown-delito", storage_type="memory")
])

# Callback para mostrar la tabla solo al hacer clic en una burbuja
from dash.dependencies import Input, Output, State

# Callback para guardar el delito seleccionado en el dropdown
@app.callback(
    Output("dropdown-delito", "data"),
    Input("mapa-crimen", "relayoutData"),
    State("dropdown-delito", "data")
)
def guardar_delito(relayoutData, prev):
    # Detectar el título del gráfico para saber el delito seleccionado
    if relayoutData and "title.text" in relayoutData:
        titulo = relayoutData["title.text"]
        # Si el título contiene un delito específico
        if "Total de Delitos: " in titulo:
            delito = titulo.replace("Total de Delitos: ", "").split(" (")[0]
            return delito
        elif "Índice de Criminalidad" in titulo:
            return None
    return prev

# Callback para mostrar la tabla solo al hacer clic en una burbuja o según el dropdown
@app.callback(
    Output("tabla-tiempos", "children"),
    Output("tabla-tiempos", "style"),
    Input("mapa-crimen", "clickData"),
    Input("dropdown-delito", "data")
)
def mostrar_tabla(clickData, delito_seleccionado):
    # Si no hay burbuja ni delito, ocultar
    if clickData is None and not delito_seleccionado:
        return None, {"display": "none"}

    # Si hay burbuja pulsada
    if clickData and "points" in clickData:
        punto = clickData["points"][0]
        boro = punto["text"]
        # Si hay delito seleccionado, filtrar por ese delito y ese borough
        if delito_seleccionado:
            df_filtrado = df[(df["BORO_NM"] == boro) & (df["TYP_DESC"] == delito_seleccionado)]
        else:
            df_filtrado = df[df["BORO_NM"] == boro]
        tabla = calcular_tiempos(df_filtrado)
        if tabla.empty:
            return html.Div(f"No hay datos para {boro}"), {"display": "block"}
        row = tabla.iloc[0]
        tabla_md = (
            html.H2(f"Tiempos Medios: {boro}", style={"textAlign": "center"}),
            dcc.Markdown(
                f"""
| Borough | Minutos llegada | Minutos duración |
|---------|----------------|------------------|
| {row['BORO_NM']} | {row['tiempo_llegada']:.2f} | {row['tiempo_duracion']:.2f} |
""", style={"fontSize": "18px", "fontFamily": "Arial"}
            )
        )
        return tabla_md, {"display": "block"}

    # Si hay delito seleccionado pero no burbuja pulsada, mostrar tabla para ese delito en todos los boroughs
    if delito_seleccionado:
        df_filtrado = df[df["TYP_DESC"] == delito_seleccionado]
        tabla = calcular_tiempos(df_filtrado)
        if tabla.empty:
            return html.Div(f"No hay datos para {delito_seleccionado}"), {"display": "block"}
        tabla_md = (
            html.H2(f"Tiempos Medios: {delito_seleccionado}", style={"textAlign": "center"}),
            dcc.Markdown(
                """
| Borough         | Minutos llegada | Minutos duración |
|-----------------|----------------|------------------|
""" + '\n'.join([
                    f"| {row['BORO_NM']} | {row['tiempo_llegada']:.2f} | {row['tiempo_duracion']:.2f} |" for _, row in tabla.iterrows()
                ]), style={"fontSize": "18px", "fontFamily": "Arial"}
            )
        )
        return tabla_md, {"display": "block"}

    return None, {"display": "none"}

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050, debug=False)