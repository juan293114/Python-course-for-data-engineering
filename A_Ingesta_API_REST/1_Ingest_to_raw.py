# Databricks notebook source
# DBTITLE 1,Instalación de paquets
# MAGIC %pip install pendulum

# COMMAND ----------

# DBTITLE 1,Importación de librerías

import json
from pathlib import Path
import pendulum
import requests

# COMMAND ----------

# DBTITLE 1,Creación de esquema y volumen
# Detecta el catálogo actual (suele ser 'workspace' o 'main')
current_catalog = spark.sql("select current_catalog()").first()[0]
catalog = current_catalog
schema = "raw"
volume = "tvmze"

# Crea esquema y volumen si no existen
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

# COMMAND ----------

# DBTITLE 1,Eliminar archivos de raw

# Elimina todos los archivos y subcarpetas dentro de la ruta
dbutils.fs.rm("/Volumes/workspace/raw/tvmze", recurse=True)


# COMMAND ----------

# DBTITLE 1,Definir parámetros
# Crear widgets
dbutils.widgets.text("start_date", "2023-01-01", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2023-12-31", "End Date (YYYY-MM-DD)")
dbutils.widgets.text("output_dir", "/Volumes/workspace/raw/tvmze", "Output Directory")
dbutils.widgets.text("api_enpoint", "https://api.tvmaze.com/schedule/web", "API endpoint")
dbutils.widgets.text("timeout", "30", "Time Out")

# Leer valores de los widgets
start_date = pendulum.parse(dbutils.widgets.get("start_date")).date()
end_date = pendulum.parse(dbutils.widgets.get("end_date")).date()
output_dir = Path(dbutils.widgets.get("output_dir"))
endpoint_dir = dbutils.widgets.get("api_enpoint")
API_URL = endpoint_dir
timeout = int(dbutils.widgets.get("timeout"))

# COMMAND ----------

# DBTITLE 1,Ingestar en la capa raw
saved_files = []
current_date = start_date

session = requests.Session()

while current_date <= end_date:
    date_str = current_date.to_date_string()

    daily_output_dir = (
        output_dir
        / f"{current_date.year:04d}"
        / f"{current_date.month:02d}"
        / f"{current_date.day:02d}"
    )
    daily_output_dir.mkdir(parents=True, exist_ok=True)

    file_path = daily_output_dir / "tvmaze.json"

    try:
        response = session.get(API_URL, params={"date": date_str}, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException:
        current_date = current_date.add(days=1)
        continue

    # Más rápido: guardar bytes del JSON tal cual lo devuelve la API
    file_path.write_bytes(response.content)

    saved_files.append(str(file_path))
    current_date = current_date.add(days=1)

print(f"Archivos guardados: {len(saved_files)}")

