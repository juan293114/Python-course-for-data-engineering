# Databricks notebook source
import pandas as pd
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

schema = StructType([
    StructField("execution_ts",  TimestampType(), True),
    StructField("job_name",      StringType(),    True),
    StructField("bronze_rows",   LongType(),      True),
    StructField("silver_rows",   LongType(),      True),
    StructField("fact_rows",     LongType(),      True),
    StructField("status",        StringType(),    True),
    StructField("error_message", StringType(),    True),
])

# Leer task values de cada capa
bronze_rows = dbutils.jobs.taskValues.get(
    taskKey="02_bronze_layer", key="bronze_rows", default=0, debugValue=0)
silver_rows = dbutils.jobs.taskValues.get(
    taskKey="03_silver_layer", key="silver_rows", default=0, debugValue=0)
fact_rows   = dbutils.jobs.taskValues.get(
    taskKey="05_fact_order_items", key="fact_rows", default=0, debugValue=0)

# Construir registro del log
log = pd.DataFrame([{
    "execution_ts"   : pd.Timestamp.now(),
    "job_name"       : "pipeline_superstore_medallion",
    "bronze_rows"    : bronze_rows,
    "silver_rows"    : silver_rows,
    "fact_rows"      : fact_rows,
    "status"         : "SUCCESS",
    "error_message"  : None
}])

# Crear schema si no existe
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.logs")

# Escribir en tabla de logs
df_spark = spark.createDataFrame(log, schema=schema)
(df_spark
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("workspace.logs.pipeline_execution_log")
)

print(f"  ✓ Log registrado: {pd.Timestamp.now()}")
