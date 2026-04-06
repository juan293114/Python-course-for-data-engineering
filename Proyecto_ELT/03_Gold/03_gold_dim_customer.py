# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.gold.dim_customer")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gold.dim_customer (
  Customer_sk BIGINT,
  Customer_ID STRING,
  Customer_Name STRING,
  Segment STRING
)
""")

# COMMAND ----------

# Columnas finales que quieres en la dimensión
columns = [
    "Customer_ID",
    "Customer_Name",
    "Segment",
]

# Leer Silver como pandas DataFrame
df = spark.read.table("workspace.silver.Super_Store").toPandas()
print(f"  ✓ Filas leídas desde Silver: {len(df):,}")

# Seleccionar columnas (si falta alguna, se crea con NaN), limpiar y ordenar
dim = (
    df
    .reindex(columns=columns)
    .dropna(subset=["Customer_ID"])
    .drop_duplicates(subset=["Customer_ID"])
    .reset_index(drop=True)
)

# Clave surrogada
dim.insert(0, "Customer_sk", range(1, len(dim) + 1))
 
print(f"  ✓ dim_customer: {len(dim):,} registros únicos")
print(dim.head())

# COMMAND ----------

# Convertir a Spark y escribir en Gold
df_spark = spark.createDataFrame(dim)
 
(df_spark
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.dim_customer")
)

print(f"  ✓ Escrita en workspace.gold.dim_customer")
