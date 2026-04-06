# Databricks notebook source
from pathlib import Path
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS workspace.silver CASCADE; 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS workspace.silver
# MAGIC COMMENT 'Capa Silver procesados'

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.silver.Super_Store (
  Row_ID INTEGER,
  Order_ID STRING,
  Order_Date DATE,
  Ship_Date DATE,
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
  Sales DOUBLE,
  Quantity INTEGER,
  Discount DOUBLE,
  Profit DOUBLE,
  Shipping_Cost DOUBLE,
  Order_Priority STRING,
  profit_margin_pct DOUBLE,
  revenue_after_discount DOUBLE,
  is_profitable BOOLEAN,
  _ingestion_ts TIMESTAMP,
  _source_file STRING,
  _pipeline_layer STRING,
  _updated_ts TIMESTAMP
)

""")

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, initcap, upper,
    to_date, datediff,
    when, round as spark_round,
    current_timestamp, lit,
    regexp_replace
)
from pyspark.sql.types import DoubleType, IntegerType
from datetime import datetime

t_start = datetime.now()
 
df_bronze = spark.read.table("workspace.bronze.Super_Store")

# COMMAND ----------

# Columnas string que necesitan trim y estandarización
str_cols = [
    "Ship_Mode", "Segment", "City", "State",
    "Country", "Market", "Region", "Category",
    "Sub_Category", "Order_Priority"
]

df_silver = df_bronze

# COMMAND ----------

for c in str_cols:
    df_silver = df_silver.withColumn(c, trim(col(c)))

# COMMAND ----------

df_silver = df_silver.withColumn(
    "Postal_Code",
    when(col("Postal_Code").isNull(), lit("N/A"))
    .when(col("Postal_Code") == "", lit("N/A"))
    .otherwise(col("Postal_Code"))
)

# COMMAND ----------

# Fechas — ajusta el formato si es necesario
DATE_FORMAT = "dd-MM-yyyy"

df_silver = (df_silver
    .withColumn("Order_Date", to_date(col("Order_Date"), DATE_FORMAT))
    .withColumn("Ship_Date",  to_date(col("Ship_Date"),  DATE_FORMAT))
)

# Tipos numéricos
df_silver = (df_silver
    .withColumn("Row_ID",        col("Row_ID").cast(IntegerType()))
    .withColumn("Quantity",      col("Quantity").cast(IntegerType()))
    .withColumn("Sales",         col("Sales").cast(DoubleType()))
    .withColumn("Discount",      col("Discount").cast(DoubleType()))
    .withColumn("Profit",        col("Profit").cast(DoubleType()))
    .withColumn("Shipping_Cost", col("Shipping_Cost").cast(DoubleType()))
)

# COMMAND ----------

# Columnas calculadas
df_silver = (df_silver
    .withColumn("profit_margin_pct",
        spark_round(
        when(col("Profit") <= 0, lit(0.0))          # ← negativo o cero → 0
            .when(col("Sales") == 0, lit(0.0))           # ← sin ventas → 0
            .otherwise(col("Profit") / col("Sales") * 100),2))
    .withColumn("revenue_after_discount",
        spark_round(col("Sales") * (lit(1.0) - col("Discount")), 2))
    .withColumn("flg_profitable",
        when(col("Profit") > 0, lit(1)).otherwise(lit(0)))
    .withColumn("_updated_ts", current_timestamp())
)


# COMMAND ----------

# Escribir en Silver
(df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.silver.Super_Store")
)

print(f"✓ Filas : {df_silver.count():,}")
print(f"✓ Cols  : {len(df_silver.columns)}")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="silver_rows", value=int(df_silver.count()))