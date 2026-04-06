# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.gold.dim_product")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gold.dim_product (
  Product_sk BIGINT,
  Product_ID STRING,
  Product_Name STRING,
  Category STRING,
  Sub_Category STRING
)
""")

# COMMAND ----------


columns = [
    "Product_ID",
    "Product_Name",
    "Category",
    "Sub_Category"
]

# Leer Silver como pandas DataFrame
df = spark.read.table("workspace.silver.Super_Store").toPandas()
print(f"  ✓ Filas leídas desde Silver: {len(df):,}")


dim = (
    df
    .reindex(columns=columns)
    .dropna(subset=["Product_ID"])
    .drop_duplicates(subset=["Product_ID"])
    .reset_index(drop=True)
)

# Clave surrogada
dim.insert(0, "Product_sk", range(1, len(dim) + 1))
 
print(f"  ✓ dim_product: {len(dim):,} registros únicos")
print(dim.head())

# COMMAND ----------

df_spark = spark.createDataFrame(dim)
 
(df_spark
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.dim_product")
)
 
print(f"  ✓ Escrita en workspace.gold.dim_product")
