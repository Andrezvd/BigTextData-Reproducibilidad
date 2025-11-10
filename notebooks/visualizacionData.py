import pandas as pd
import glob
import os


#ESTE ARCHIVO ES PARA VISUALIZAR LOS DATOS VECTORIZADOS EN LOCAL NO SE USA EN CLUSTER UIS

# Detectar ruta base (ajústala si tu carpeta está en otro sitio)
base_path = r"C:\Users\andre\Desktop\Reproducibilidad-Big-Tex-Data\docker\vectorized_tfidf_local"

# Mostrar en qué carpeta estamos
print("Carpeta actual:", os.getcwd())

# Buscar recursivamente todos los .parquet
archivos = glob.glob(os.path.join(base_path, "**", "*.parquet"), recursive=True)
print(f"Se encontraron {len(archivos)} archivos parquet")

# Verifica que haya archivos
if not archivos:
    raise FileNotFoundError("⚠️ No se encontraron archivos .parquet. Verifica la ruta 'base_path'.")

# Leer y concatenar todos los archivos
print("Leyendo archivos...")
df = pd.concat([pd.read_parquet(f) for f in archivos])
print("Data cargada correctamente. Dimensiones:", df.shape)

# Mostrar primeras filas
print(df.head())
