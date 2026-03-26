# Databricks notebook source
# DBTITLE 1,Instalación de paquets
# MAGIC %pip install kaggle
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Creación de esquema y volumen
# Detecta el catálogo actual
current_catalog = spark.sql("select current_catalog()").first()[0]

catalog = current_catalog
schema = "raw"
volume = "kaggle_adidas_US_sales"
dataset_code = "sagarmorework/adidas-us-sales"

# Crea esquema y volumen si no existen
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

# COMMAND ----------

# DBTITLE 1,Importación de librerías y conexión a API
import os
from kaggle.api.kaggle_api_extended import KaggleApi

#Llamada del scope con los secret (apikey y user de kaggle)
username = dbutils.secrets.get(scope = "kaggle_api", key = "kaggle_user")
key = dbutils.secrets.get(scope = "kaggle_api", key = "kaggle_api_key")

#Se llevan a la variable de entorno los secret de kaggle
os.environ['KAGGLE_USERNAME'] = username
os.environ['KAGGLE_KEY'] = key

#Autenticacion con la api de Kaggle
api = KaggleApi()
api.authenticate()

# COMMAND ----------

# DBTITLE 1,Ingesta del RAW

#Descarga el dataset desde Kaggle, y descomprime su contenido
api.dataset_download_files(
    dataset_code, 
    path=f"/Volumes/{catalog}/{schema}/{volume}", 
    force=True, 
    unzip=True
)
