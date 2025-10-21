import pandas as pd
import mysql.connector


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
    print(f"Archivo cargado con {len(df_limpio)} filas.")
    
    df_limpio['MachineID'] = df_limpio['MachineID'].astype(str).str.strip() 
    
except FileNotFoundError:
    print(f"Error: El archivo '{NOMBRE_ARCHIVO_LIMPIO}' no se encontró.")
    exit()

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("Conexión a la base de datos exitosa.")
except mysql.connector.Error as err:
    print(f"Error al conectar a MySQL: {err}")
    exit()



def cargar_dim_maquina_1x1(conn, cursor, df_origen):
    """Inserta nuevos valores únicos de MachineID uno por uno."""
    
    tabla_dim = 'Dim_Maquina'
    col_csv = 'MachineID'
    col_dim = 'machine_id' 
    
    print(f"\n[INICIO] Carga de {tabla_dim}...")

    valores_csv = df_origen[col_csv].unique()
    
    cursor.execute(f"SELECT {col_dim} FROM {tabla_dim}")
    valores_bd = {row[0] for row in cursor.fetchall()}
    
    valores_a_insertar = [v for v in valores_csv if v not in valores_bd]
    
    if not valores_a_insertar:
        print(f"-> {tabla_dim}: No hay IDs de máquina nuevos que insertar.")
        return

    print(f"-> {tabla_dim}: Insertando {len(valores_a_insertar)} IDs nuevos (1x1)...")
    
    sql_insert = f"INSERT INTO {tabla_dim} ({col_dim}) VALUES (%s)"
    registros_insertados = 0
    
    for valor in valores_a_insertar:
        try:
            cursor.execute(sql_insert, (valor,))
            registros_insertados += 1
        except mysql.connector.Error as err:
            print(f"[Error en {tabla_dim}] No se pudo insertar '{valor}': {err}") 
            
    conn.commit()
    print(f"-> {tabla_dim} cargada con éxito. Total insertado: {registros_insertados}")


cargar_dim_maquina_1x1(conn, cursor, df_limpio)

cursor.close()
conn.close()
print("Proceso de carga de Dim_Maquina finalizado.")