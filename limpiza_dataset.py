import pandas as pd
import numpy as np
from datetime import datetime

NOMBRE_ARCHIVO_ENTRADA = 'dataset_original.csv'
NOMBRE_ARCHIVO_SALIDA = 'datos_limpios_listos_para_dw.csv'

FORMATO_ENTRADA_FECHA = '%d/%m/%Y'
FORMATO_SALIDA_DW = '%Y-%m-%d'
FECHA_POR_DEFECTO = datetime(1900, 1, 1).date()

VALORES_VALIDOS_TURNO = {'mañana', 'tarde', 'noche'}


def limpiar_datos_para_dw(nombre_entrada, nombre_salida):
    """
    Carga un CSV, aplica reglas de limpieza columna por columna y guarda el resultado.
    """
    print(f"Iniciando limpieza del archivo: {nombre_entrada}")
    
    try:
        df = pd.read_csv(nombre_entrada)
        filas_iniciales = len(df)
        print(f"Cargadas {filas_iniciales} filas.")
        
        print("-> Aplicando reglas a 'Fecha'...")
        
        df['Fecha'] = pd.to_datetime(
            df['Fecha'],
            format=FORMATO_ENTRADA_FECHA,
            errors='coerce' 
        )
        
        df['Fecha'].fillna(FECHA_POR_DEFECTO, inplace=True)
        
        df['Fecha'] = pd.to_datetime(df['Fecha']).dt.strftime(FORMATO_SALIDA_DW)

        print("-> Aplicando reglas a 'Piezas_Producidas' y 'Piezas_Defectuosas'...")
        columnas_numericas = ['Piezas_Producidas', 'Piezas_Defectuosas']
        
        for col in columnas_numericas:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df[col].fillna(0, inplace=True)
            
            df.loc[df[col] < 0, col] = 0
            

        print("-> Aplicando reglas a 'Turno'...")
        

        df['Turno'] = df['Turno'].astype(str).str.lower().str.strip()
        
        df['Turno'] = df['Turno'].apply(
            lambda x: x if x in VALORES_VALIDOS_TURNO else np.nan
        )

        df['Turno'] = df['Turno'].str.capitalize()
  
        df.drop_duplicates(inplace=True)
        filas_finales = len(df)
        
        df.to_csv(nombre_salida, index=False)
        
        print("\n--- Tarea Completada con Éxito ---")
        print(f"Filas originales: {filas_iniciales}")
        print(f"Filas después de la limpieza: {filas_finales}")
        print(f"Archivo limpio listo para DW guardado como: '{nombre_salida}'")

    except FileNotFoundError:
        print(f"\n[ERROR FATAL] El archivo '{nombre_entrada}' no se encontró. Asegúrate de que esté en la misma carpeta.")
    except Exception as e:
        print(f"\n[ERROR FATAL] Ocurrió un error inesperado durante la limpieza: {e}")


if __name__ == "__main__":
    limpiar_datos_para_dw(NOMBRE_ARCHIVO_ENTRADA, NOMBRE_ARCHIVO_SALIDA)