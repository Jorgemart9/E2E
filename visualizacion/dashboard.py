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
        'TYP_DESC': ['ROBBERY', 'BURGLARY', 'ASSAULT', 'LARCENY', 'DRUGS', 'ROBBERY'] * 20
    }
    df = pd.DataFrame(data)

# Limpieza básica
df = df.dropna(subset=['BORO_NM'])
df['BORO_NM'] = df['BORO_NM'].astype(str).str.upper().str.strip()

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

app.layout = html.Div([
    html.H1("Análisis de Criminalidad NYPD", style={"textAlign": "center", "fontFamily": "Arial"}),
    dcc.Graph(figure=fig, style={"height": "90vh"})
])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050, debug=False)