import pandas as pd

# Leer el CSV
df = pd.read_csv('NYPD_Calls_for_Service_(Year_to_Date)_20251209-2.csv')

# Crear dataframe agrupado por barrio (BORO_NM) y tipo de delito (TYP_DESC) con count
df_agrupado = df.groupby(['BORO_NM', 'TYP_DESC']).size().reset_index(name='count')


