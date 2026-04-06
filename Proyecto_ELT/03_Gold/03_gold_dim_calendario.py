# Databricks notebook source
import pandas as pd
from datetime import datetime

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.gold.dim_calendar")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gold.dim_calendar (
  Date DATE,
  year INT,
  quarter TINYINT,
  quarter_name STRING,
  month_num TINYINT,
  month_name STRING,
  week_of_year INT,
  day_num INT,
  flg_weekend TINYINT,
  period STRING
)
""")

# COMMAND ----------


df_silver = spark.read.table("workspace.silver.Super_Store").toPandas()
df_silver["Order_Date"] = pd.to_datetime(df_silver["Order_Date"])
df_silver["Ship_Date"]  = pd.to_datetime(df_silver["Ship_Date"])

# Fecha mínima entre Order_Date y Ship_Date
fecha_inicio_raw = min(
    df_silver["Order_Date"].min(),
    df_silver["Ship_Date"].min()
)

# Fecha máxima entre ambas columnas
fecha_fin_raw = max(
    df_silver["Order_Date"].max(),
    df_silver["Ship_Date"].max()
)

# Ajustar inicio al primer día del mes más antiguo
fecha_inicio = fecha_inicio_raw.replace(day=1).strftime("%Y-%m-%d")

# Ajustar fin al 31 de diciembre del año en curso
fecha_fin = f"{fecha_fin_raw.year}-12-31"

# COMMAND ----------

fechas = pd.date_range(start=fecha_inicio, end=fecha_fin, freq="D")

dim_calendar = pd.DataFrame({"Date": fechas})

# Atributos derivados
dim_calendar["year"]          = dim_calendar["Date"].dt.year
dim_calendar["quarter"]       = dim_calendar["Date"].dt.quarter.astype("int8")
dim_calendar["quarter_name"]  = "Q" + dim_calendar["quarter"].astype(str)
dim_calendar["month_num"]     = dim_calendar["Date"].dt.month.astype("int8")
dim_calendar["month_name"]    = dim_calendar["Date"].dt.strftime("%B")
dim_calendar["week_of_year"]  = dim_calendar["Date"].dt.isocalendar().week.astype(int)
dim_calendar["day_num"]       = dim_calendar["Date"].dt.day
dim_calendar["flg_weekend"]    = (dim_calendar["Date"].dt.dayofweek >= 5).astype("int8")
dim_calendar["period"]    = dim_calendar["Date"].dt.strftime("%Y%m")

print(f"  ✓ dim_calendar: {len(dim_calendar):,} fechas consecutivas")
print(f"  ✓ Desde: {fecha_inicio} → Hasta: {fecha_fin}")
print(dim_calendar.head())

# COMMAND ----------

df_spark = spark.createDataFrame(dim_calendar)

(df_spark
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.dim_calendar")
)

print(f"  ✓ dim_calendar escrita: {len(dim_calendar):,} fechas")
