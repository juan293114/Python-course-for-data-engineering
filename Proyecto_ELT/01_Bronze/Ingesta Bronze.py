# Databricks notebook source
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from datetime import datetime

# COMMAND ----------

from pathlib import Path
import pandas as pd

# COMMAND ----------

dbutils.widgets.text("raw_root", "/Volumes/workspace/default/capa_raw/Global_Superstore2.csv", "Ruta del .CSV")
dbutils.widgets.text("bronze_table",  "workspace.bronze.super_store", "Tabla Delta destino")

RAW_FILE_PATH = dbutils.widgets.get("raw_root")
BRONZE_TABLE  = dbutils.widgets.get("bronze_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS workspace.bronze CASCADE; 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS workspace.bronze
# MAGIC COMMENT 'Capa Bronze: datos crudos procesados'

# COMMAND ----------

# Eliminar la tabla anterior y recrear limpia
spark.sql("DROP TABLE IF EXISTS workspace.bronze.Super_Store")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.bronze.Super_Store (
  Row_ID STRING,
  Order_ID STRING,
  Order_Date STRING,
  Ship_Date STRING,
  Ship_Mode STRING,
  Customer_ID STRING,
  Customer_Name STRING,
  Segment STRING,
  City STRING,
  State STRING,
  Country STRING,
  Postal_Code STRING,
  Market STRING,
  Region STRING,
  Product_ID STRING,
  Category STRING,
  Sub_Category STRING,
  Product_Name STRING,
  Sales STRING,
  Quantity STRING,
  Discount STRING,
  Profit STRING,
  Shipping_Cost STRING,
  Order_Priority STRING
)

""")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# Leer TODO como string en Bronze — sin inferSchema
df_raw = (spark.read
    .option("header",      "true")
    .option("inferSchema", "false")   # ← clave: todo llega como string
    .option("multiLine",   "true")
    .option("quote",       '"')
    .option("escape",      '"')
    .option("encoding",    "ISO-8859-1") 
    .csv(RAW_FILE_PATH)
)

# Renombrar columnas: espacios y guiones → guion bajo
df_bronze = df_raw
for col_name in df_raw.columns:
    new_name = col_name.replace(" ", "_").replace("-", "_")
    df_bronze = df_bronze.withColumnRenamed(col_name, new_name)

print("✓ Columnas:", df_bronze.columns)
print("✓ Filas   :", df_bronze.count())

# COMMAND ----------

# Agregar auditoría
df_bronze = (df_bronze
    .withColumn("_ingestion_ts",   current_timestamp())
    .withColumn("_source_file",    lit("Global_Superstore2.csv"))
    .withColumn("_pipeline_layer", lit("bronze"))
)

# COMMAND ----------

(df_bronze.write
 .format("delta")
 .mode("overwrite")
 .option("mergeSchema", "true")
 .saveAsTable(BRONZE_TABLE)
)

print(f"✓ Tabla recreada con {df_bronze.count():,} filas")
print(f"✓ Columnas: {len(df_bronze.columns)}")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="bronze_rows", value=int(df_bronze.count()))
