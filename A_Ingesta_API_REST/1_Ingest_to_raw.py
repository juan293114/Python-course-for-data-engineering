# Databricks notebook source
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

# DBTITLE 1,Importar dependecias y definir parámetros
# MAGIC %pip install pendulum
# MAGIC import json
# MAGIC import logging
# MAGIC from pathlib import Path
# MAGIC from typing import List, Sequence
# MAGIC import pendulum
# MAGIC import requests
# MAGIC
# MAGIC # Crear widgets
# MAGIC dbutils.widgets.text("start_date", "2023-01-01", "Start Date (YYYY-MM-DD)")
# MAGIC dbutils.widgets.text("end_date", "2023-12-31", "End Date (YYYY-MM-DD)")
# MAGIC dbutils.widgets.text("output_dir", "/Volumes/workspace/raw/tvmze", "Output Directory")
# MAGIC dbutils.widgets.text("timeout", "300", "Timeout (seconds)")
# MAGIC
# MAGIC # Leer valores de los widgets
# MAGIC start_date = pendulum.parse(dbutils.widgets.get("start_date")).date()
# MAGIC end_date = pendulum.parse(dbutils.widgets.get("end_date")).date()
# MAGIC output_dir = Path(dbutils.widgets.get("output_dir"))
# MAGIC timeout = int(dbutils.widgets.get("timeout"))

# COMMAND ----------

# DBTITLE 1,Ingestar en la capa raw
API_URL = "http://api.tvmaze.com/schedule/web"

logger = logging.getLogger(__name__)


saved_files: List[str] = []

current_date = start_date
while current_date <= end_date:
    params = {"date": current_date.to_date_string()}
    try:
        response = requests.get(API_URL, params=params, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("TVMaze API unavailable for %s: %s", current_date.to_date_string(), exc)
        current_date = current_date.add(days=1)
        continue

    daily_output_dir = (
        output_dir / f"{current_date.year:04d}" / f"{current_date.month:02d}" / f"{current_date.day:02d}"
    )

    daily_output_dir.mkdir(parents=True, exist_ok=True)

    file_path = daily_output_dir / "tvmaze.json"
    file_path.write_text(json.dumps(response.json(), indent=2), encoding="utf-8")
    saved_files.append(str(file_path))

    current_date = current_date.add(days=1)
