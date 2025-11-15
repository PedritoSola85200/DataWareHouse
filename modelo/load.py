# El script limpia el dataset original, crea una variable de defecto y extrae datos de fecha para análisis.

import pandas as pd

# Cargar dataset
df = pd.read_csv('dataset_original.csv', sep='\,')  # Usa sep=',' si el archivo está separado por comas

# Ver las primeras filas
print(df.head())

# Crear variable objetivo: 1 si hay defectos, 0 si no
df['Defecto'] = (df['Piezas_Defectuosas'] > 0).astype(int)

# Convertir la fecha a formato datetime
df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y')

# Crear variables de calendario
df['Año'] = df['Fecha'].dt.year
df['Mes'] = df['Fecha'].dt.month
df['Día'] = df['Fecha'].dt.day
df['Día_Semana'] = df['Fecha'].dt.dayofweek

print("\nColumnas finales del dataset:")
print(df.columns)
