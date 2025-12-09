import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html

df = pd.read_csv('NYPD_Calls_for_Service_(Year_to_Date)_20251209-2.csv')
df = pd.DataFrame(df)
df = pd.DataFrame(df)

df_agrupado = df.groupby(['BORO_NM', 'TYP_DESC']).size().reset_index(name='count')

coords_data = {
    'BORO_NM': ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND'],
    'Latitud': [40.8448, 40.6782, 40.7831, 40.7282, 40.5795],
    'Longitud': [-73.8648, -73.9442, -73.9712, -73.7949, -74.1502]
}
df_coords = pd.DataFrame(coords_data)

df_final = pd.merge(df_agrupado, df_coords, on='BORO_NM', how='left')

# Obtener valores mín y máx para la escala de colores
min_count = df_final['count'].min()
max_count = df_final['count'].max()

fig = go.Figure()

for tipo_delito in df_final['TYP_DESC'].unique():
    df_tipo = df_final[df_final['TYP_DESC'] == tipo_delito]
    colors = [(df_tipo['count'].values[i] - min_count) / (max_count - min_count) for i in range(len(df_tipo))]
    fig.add_trace(go.Scattermapbox(
        lat=df_tipo['Latitud'],
        lon=df_tipo['Longitud'],
        mode='markers+text',
        marker=dict(
            size=(df_tipo['count'] / max_count) * 120 + 30,  # Mucho más grandes
            color=df_tipo['count'],
            colorscale='RdYlGn_r',
            showscale=False,
            cmin=min_count,
            cmax=max_count,
            opacity=0.8
        ),
        text=df_tipo['BORO_NM'],
        textposition='top center',
        hovertemplate='<b>%{text}</b><br>Delitos: %{customdata}<extra></extra>',
        customdata=df_tipo['count'],
        name=tipo_delito
    ))

fig.update_layout(
    mapbox=dict(
        style='carto-positron',
        zoom=9,
        center=dict(lat=40.7128, lon=-74.0060)
    ),
    margin=dict(r=0, t=80, l=0, b=0),
    title="Mapa de Llamadas NYPD (Filtrable)",
    showlegend=False
)

buttons = []

buttons.append(dict(
    label="Todos",
    method="update",
    args=[
        {"visible": [True] * len(fig.data)},
        {"title": "Todos los delitos", "showlegend": False}
    ]
))

for i, tipo_delito in enumerate(df_final['TYP_DESC'].unique()):
    visibilidad = [False] * len(fig.data)
    visibilidad[i] = True
    
    # Obtener el total de delitos de este tipo
    total_delitos_tipo = df_final[df_final['TYP_DESC'] == tipo_delito]['count'].sum()
    
    buttons.append(dict(
        label=f"{tipo_delito} ({total_delitos_tipo})",
        method="update",
        args=[
            {"visible": visibilidad},
            {"title": f"Delito: {tipo_delito} - Total: {total_delitos_tipo}", "showlegend": False}
        ]
    ))

fig.update_layout(
    updatemenus=[
        dict(
            buttons=buttons,
            direction="down",
            pad={"r": 10, "t": 10},
            showactive=True,
            x=0.1,
            xanchor="left",
            y=1.1,
            yanchor="top"
        ),
    ],
    margin={"r":0,"t":80,"l":0,"b":0}
)

# Crear la aplicación Dash
app = Dash(__name__)

app.layout = html.Div([
    html.H1("Mapa de Llamadas NYPD", style={"textAlign": "center", "marginBottom": 20}),
    dcc.Graph(figure=fig, style={"height": "90vh"})
])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050, debug=False)