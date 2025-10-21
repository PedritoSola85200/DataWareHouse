import pandas as pd
import mysql.connector

DB_CONFIG = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    'database': 'datawarehouse',
    'charset': 'utf8mb4'
}

df_limpio = pd.read_csv('datos_limpios_listos_para_dw.csv', encoding='utf-8')
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

operadores_unicos = df_limpio['Operador'].dropna().unique()

for nombre in operadores_unicos:
    sql = """
        INSERT INTO Dim_Operador (nombre_operador) 
        VALUES (%s) 
        ON DUPLICATE KEY UPDATE nombre_operador = VALUES(nombre_operador);
    """
    try:
        cursor.execute(sql, (nombre.capitalize(),))
    except mysql.connector.Error as err:
        print(f"Error al insertar operador {nombre}: {err}")

conn.commit()
print("Dim_Operador cargada con éxito.")
cursor.close()
conn.close()