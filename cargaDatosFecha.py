# Este script genera la tabla Dim_Tiempo para el año actual e inserta solo las fechas que faltan.

import pandas as pd
import mysql.connector
import locale
from datetime import datetime


ANIO_ACTUAL = datetime.now().year

FECHA_INICIO_DW = f'{ANIO_ACTUAL}-01-01' 
FECHA_FIN_DW = f'{ANIO_ACTUAL}-12-31'   

NOMBRE_ARCHIVO_LIMPIO = 'datos_limpios_listos_para_dw.csv' 

DB_CONFIG = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    'database': 'datawarehouse',
    'charset': 'utf8mb4'
}

try:
    df_limpio = pd.read_csv(NOMBRE_ARCHIVO_LIMPIO, encoding='utf-8')
    print(f"Archivo de datos cargado con {len(df_limpio)} filas. (No usado para Dim_Tiempo)")
except FileNotFoundError:
    print(f"Advertencia: El archivo '{NOMBRE_ARCHIVO_LIMPIO}' no se encontró. No afecta la carga de Dim_Tiempo.")
    df_limpio = pd.DataFrame() 

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("Conexión a la base de datos exitosa.")
except mysql.connector.Error as err:
    print(f"Error al conectar a MySQL: {err}")
    exit()



def generar_dim_tiempo_df(fecha_inicio: str, fecha_fin: str) -> pd.DataFrame:
    """Genera el DataFrame completo de Dim_Tiempo para el rango especificado."""
    
    df = pd.DataFrame({'fecha': pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D')})
    df['id_fecha'] = df['fecha'].dt.strftime('%Y%m%d').astype(int)
    df['anio'] = df['fecha'].dt.year
    df['mes'] = df['fecha'].dt.month
    df['dia'] = df['fecha'].dt.day

    try:
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8') 
        df['nombre_dia'] = df['fecha'].dt.strftime('%A').str.capitalize()
        df['nombre_mes'] = df['fecha'].dt.strftime('%B').str.capitalize()
    except Exception:
        print("Advertencia: Falló el locale 'es_ES.UTF-8'. Usando nombres en español alternativos.")
        df['nombre_dia'] = df['fecha'].dt.day_name(locale='es_ES.UTF-8', errors='coerce').fillna(df['fecha'].dt.day_name()).str.capitalize()
        df['nombre_mes'] = df['fecha'].dt.month_name(locale='es_ES.UTF-8', errors='coerce').fillna(df['fecha'].dt.month_name()).str.capitalize()
        
    dim_tiempo_df = df[['id_fecha', 'fecha', 'dia', 'mes', 'anio', 'nombre_dia', 'nombre_mes']]
    return dim_tiempo_df


def cargar_dim_tiempo(conn, cursor, df_dim_tiempo):
    """Carga el DataFrame de Dim_Tiempo en la tabla MySQL."""
    
    tabla_dim = 'Dim_Tiempo'
    print(f"\n[INICIO] Carga de {tabla_dim}...")

    try:
        cursor.execute(f"SELECT id_fecha FROM {tabla_dim}")
        valores_bd = {row[0] for row in cursor.fetchall()}
    except mysql.connector.Error as err:
        print(f"Error al leer {tabla_dim}: {err}. Asegúrese de que la tabla existe.")
        return

    valores_df = set(df_dim_tiempo['id_fecha'].tolist())
    ids_a_insertar = list(valores_df - valores_bd)
    
    if not ids_a_insertar:
        print(f"-> {tabla_dim}: No hay fechas nuevas para insertar en el rango {FECHA_INICIO_DW} - {FECHA_FIN_DW}.")
        return

    df_a_insertar = df_dim_tiempo[df_dim_tiempo['id_fecha'].isin(ids_a_insertar)]
    print(f"-> {tabla_dim}: Insertando {len(df_a_insertar)} fechas nuevas para el año {ANIO_ACTUAL}...")
    
    columnas_bd = '(id_fecha, fecha, dia, mes, anio, nombre_dia, nombre_mes)'
    placeholders = '(%s, %s, %s, %s, %s, %s, %s)'
    sql_insert = f"INSERT INTO {tabla_dim} {columnas_bd} VALUES {placeholders}"
    
    registros_insertados = 0
    for _, row in df_a_insertar.iterrows():
        try:
            datos = (
                row['id_fecha'], 
                row['fecha'].strftime('%Y-%m-%d'), 
                row['dia'], 
                row['mes'], 
                row['anio'], 
                row['nombre_dia'], 
                row['nombre_mes']
            )
            cursor.execute(sql_insert, datos)
            registros_insertados += 1
        except mysql.connector.Error as err:
            print(f"[Error en {tabla_dim}] No se pudo insertar fecha {row['fecha']}: {err}") 
            
    conn.commit()
    print(f"-> {tabla_dim} cargada con éxito. Total insertado: {registros_insertados}")

dim_tiempo_df = generar_dim_tiempo_df(FECHA_INICIO_DW, FECHA_FIN_DW)

cargar_dim_tiempo(conn, cursor, dim_tiempo_df)

cursor.close()
conn.close()
print("\nProceso de carga de Dim_Tiempo finalizado.")
