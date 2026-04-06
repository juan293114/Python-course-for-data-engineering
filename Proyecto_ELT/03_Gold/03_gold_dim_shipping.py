# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.gold.dim_shipping")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gold.dim_shipping (
  Ship_sk BIGINT,
  Ship_Mode STRING
)
""")

# COMMAND ----------

# Columnas finales que quieres en la dimensión
columns = [
    "Ship_Mode",
]

# Leer Silver como pandas DataFrame
df = spark.read.table("workspace.silver.Super_Store").toPandas()
print(f"  ✓ Filas leídas desde Silver: {len(df):,}")


dim = (
    df
    .reindex(columns=columns)
    .dropna(subset=["Ship_Mode"])
    .drop_duplicates(subset=["Ship_Mode"])
    .reset_index(drop=True)
)

# Clave surrogada
dim.insert(0, "Ship_sk", range(1, len(dim) + 1))
 
print(f"  ✓ dim_shipping: {len(dim):,} registros únicos")
print(dim.head())

# COMMAND ----------

df_spark = spark.createDataFrame(dim)
 
(df_spark
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.dim_shipping")
)
 
print(f"  ✓ Escrita en workspace.gold.dim_shipping")
