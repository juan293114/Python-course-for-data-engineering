# Databricks notebook source
# DBTITLE 1,Importación de librerías

import json
from pathlib import Path
import requests
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import os

# COMMAND ----------

# DBTITLE 1,Declaración de rutas
# Detecta el catálogo actual (suele ser 'workspace' o 'main')
current_catalog = spark.sql("select current_catalog()").first()[0]
catalog = current_catalog
schema = "raw"
volume = "proyecto"

# COMMAND ----------

# DBTITLE 1,Creación de esquema y volumen

# Crea esquema y volumen si no existen
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

# COMMAND ----------

# DBTITLE 1,Creación de widgets
dbutils.widgets.text("start_date", "", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End Date (YYYY-MM-DD)")
dbutils.widgets.text("output_dir", "/Volumes/workspace/raw/proyecto/openweathermap", "Output Directory")
dbutils.widgets.text("api_token", "")

# COMMAND ----------

# API key
api_key = dbutils.widgets.get("api_token")

# COMMAND ----------

# Obtener parámetros de los widgets (en UTC)
start_date_str = dbutils.widgets.get("start_date")  # ej. '2026-04-06T00:00:00'
end_date_str   = dbutils.widgets.get("end_date")    # ej. '2026-04-06T23:59:59'

# Convertir a objetos datetime
start_date_utc = datetime.fromisoformat(start_date_str)
end_date_utc   = datetime.fromisoformat(end_date_str)

# Ajustar a Lima (UTC-5)
start_date = start_date_utc - timedelta(hours=5)
end_date   = end_date_utc - timedelta(hours=5)

print("Start date Lima:", start_date)
print("End date Lima:", end_date)

# COMMAND ----------


output_dir = Path(dbutils.widgets.get("output_dir"))

# COMMAND ----------

# DBTITLE 1,Ingestar en la capa raw
# Leer CSV con distritos y coordenadas
df_geo = pd.read_csv("/Volumes/workspace/raw/proyecto/data_ubigeo_1.csv", sep=";", encoding="utf-8")

# Bucle día por día
print("procesando desde : ", start_date, " hasta: ", end_date )
current_date = start_date
while current_date <= end_date:
    # Definir rango de 24 horas
    start_ts = int(current_date.timestamp()) 
    end_ts = int((current_date + timedelta(days=1)).timestamp())

    print(f"Procesando día: {current_date.strftime('%Y-%m-%d')}")

    registros_dia = []

    # Loop por cada distrito
    for _, row in df_geo.iterrows():
        lat = row["latitud"]
        lon = row["longitud"]
        distrito = row["distrito"]
        provincia = row["provincia"]
        departamento = row["departamento"]
        ubigeo = row["ubigeo_Inei"]

        # Llamada a la API para ese día
        url = f"http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start_ts}&end={end_ts}&appid={api_key}"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            records = data.get("list", [])

            # Enriquecer con info geográfica
            for r in records:
                r["ubigeo"] = ubigeo
                r["distrito"] = distrito
                r["provincia"] = provincia
                r["departamento"] = departamento
                r["latitud"] = lat
                r["longitud"] = lon
                registros_dia.append(r)
        else:
            print(f"Error {response.status_code} en {distrito}")

    # Guardar todos los registros de ese día
    y, m, d = current_date.strftime("%Y-%m-%d").split("-")
    path = f"{output_dir}/{y}/{m}/{d}/air_pollution.json"

    # Si existe un archivo previo, lo borra
    if os.path.exists(path):
        os.remove(path)
        print(f"Archivo existente eliminado: {path}")

    # Crear directorio si no existe
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Guardar nuevo archivo JSON
    pd.DataFrame(registros_dia).to_json(path, orient="records", lines=True, force_ascii=False)
    print(f"Guardado: {path}")

    # Avanzar al siguiente día
    current_date += timedelta(days=1)
