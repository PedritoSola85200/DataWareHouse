import pandas as pd
import mysql.connector
import numpy as np

NOMBRE_ARCHIVO_LIMPIO = 'datos_limpios_listos_para_dw.csv'

DB_CONFIG = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    'database': 'datawarehouse',
    'charset': 'utf8mb4'
}

import pandas as pd
import mysql.connector
import numpy as np


def conectar_db():
    """Establece la conexión a la base de datos."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn, conn.cursor()
    except mysql.connector.Error as err:
        print(f"\n[ERROR FATAL] Fallo al conectar a MySQL: {err}")
        return None, None

def cargar_tabla_hechos_final(conn, cursor, df_origen):
    """
    Carga los hechos usando mapeo por NOMBRE (SCD Tipo 0) para todas las dimensiones,
    incluyendo Operador.
    """
    print("\n[FASE FINAL] Iniciando mapeo y carga de Hechos_Produccion (Mapeo por Nombre)...")
    
    df_hechos = df_origen.copy()

    def mapear_id(df, tabla_dim, col_id, col_dim_nombre, col_csv, should_capitalize=True):
        """Consulta la BD y fusiona el ID en el DataFrame de hechos basado en el nombre."""
        sql = f"SELECT {col_id}, {col_dim_nombre} FROM {tabla_dim}"
        df_map = pd.read_sql(sql, conn)
        
        left_col_name = col_csv + '_KEY' 
        if should_capitalize:
            df[left_col_name] = df[col_csv].astype(str).str.strip().str.capitalize()
        else:
            df[left_col_name] = df[col_csv].astype(str).str.strip()
            
        df_merged = pd.merge(df, df_map, 
                             left_on=left_col_name, 
                             right_on=col_dim_nombre, 
                             how='left')
                             
        df_merged = df_merged.drop(columns=[col_dim_nombre, left_col_name, col_csv])
        
        return df_merged
    
    df_hechos = mapear_id(df_hechos, 'Dim_Operador', 'id_operador', 'nombre_operador', 'Operador')
    
    df_hechos = mapear_id(df_hechos, 'Dim_Turno', 'id_turno', 'nombre_turno', 'Turno')
    
    df_hechos = mapear_id(df_hechos, 'Dim_Linea', 'id_linea', 'nombre_linea', 'Linea') 
    
    df_hechos = mapear_id(df_hechos, 'Dim_Maquina', 'id_maquina', 'machine_id', 'MachineID', should_capitalize=False)

    sql_tiempo = "SELECT id_fecha, DATE_FORMAT(fecha, '%Y-%m-%d') AS fecha_str FROM Dim_Tiempo"
    df_map_tiempo = pd.read_sql(sql_tiempo, conn)
    
    df_hechos = pd.merge(df_hechos, df_map_tiempo, 
                         left_on=df_hechos['Fecha'].astype(str), 
                         right_on='fecha_str', 
                         how='left').drop(columns=['fecha_str', 'Fecha'])

    
    df_hechos = df_hechos.rename(columns={
        'Piezas_Producidas': 'piezas_producidas', 
        'Piezas_Defectuosas': 'piezas_defectuosas'
    })
    
    columnas_finales = ['id_fecha', 'id_linea', 'id_maquina', 'id_operador', 'id_turno', 
                        'piezas_producidas', 'piezas_defectuosas']
    
    df_hechos = df_hechos[columnas_finales].dropna().astype(int, errors='ignore').copy()

    datos_a_insertar = df_hechos.values.tolist()
    
    if not datos_a_insertar:
        print("¡Advertencia! No hay datos válidos de hechos para insertar después del mapeo.")
        return

    sql_insert_hechos = """
        INSERT INTO Hechos_Produccion (id_fecha, id_linea, id_maquina, id_operador, id_turno, piezas_producidas, piezas_defectuosas)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor.executemany(sql_insert_hechos, datos_a_insertar)
    conn.commit()
    print(f"-> Hechos_Produccion: Insertadas {len(datos_a_insertar)} filas de hechos con éxito. ¡Carga DW COMPLETADA! 🎉")


if __name__ == "__main__":
    
    conn, cursor = conectar_db()
    if conn is None:
        exit()

    try:
        df_limpio = pd.read_csv(NOMBRE_ARCHIVO_LIMPIO, encoding='utf-8')
        
        df_limpio['Operador'] = df_limpio['Operador'].astype(str).str.strip().str.capitalize()
        df_limpio['Turno'] = df_limpio['Turno'].astype(str).str.strip().str.capitalize()
        df_limpio['Linea'] = df_limpio['Linea'].astype(str).str.strip().str.capitalize() 
        df_limpio['MachineID'] = df_limpio['MachineID'].astype(str).str.strip()
        df_limpio['Fecha'] = pd.to_datetime(df_limpio['Fecha']).dt.strftime('%Y-%m-%d')
        print(f"Archivo limpio cargado y estandarizado con {len(df_limpio)} filas.")
        
    except Exception as e:
        print(f"\n[ERROR FATAL] Error en la carga/estandarización: {e}")
        conn.close()
        exit()


    cargar_tabla_hechos_final(conn, cursor, df_limpio)

    cursor.close()
    conn.close()
    print("\nConexión cerrada.")