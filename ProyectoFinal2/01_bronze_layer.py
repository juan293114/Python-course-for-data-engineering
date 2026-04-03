# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer — Ingesta de Datos Crudos
# MAGIC ## Pipeline ELT | Arquitectura Medallón | Olist Brazilian E-Commerce
# MAGIC
# MAGIC **Objetivo:** Leer los archivos CSV desde la capa Raw (Volumen DBFS) y cargarlos
# MAGIC como Delta Tables en el esquema Bronze, homogenizando el formato de almacenamiento
# MAGIC sin aplicar transformaciones sobre el contenido.
# MAGIC
# MAGIC **Fuente:** Kaggle - Brazilian E-Commerce Public Dataset by Olist
# MAGIC https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración de Parámetros y Rutas

# COMMAND ----------

# Parámetros configurables (se pueden inyectar desde el Job de Databricks)
dbutils.widgets.text("raw_path", "/Volumes/workspace/raw/olist_data", "Raw Data Path")
dbutils.widgets.text("bronze_schema", "bronze_ecommerce", "Bronze Schema Name")
dbutils.widgets.text("catalog", "workspace", "Unity Catalog Name")

RAW_PATH     = dbutils.widgets.get("raw_path")
BRONZE_DB    = dbutils.widgets.get("bronze_schema")
CATALOG      = dbutils.widgets.get("catalog")

print(f"📂 Raw Path    : {RAW_PATH}")
print(f"🥉 Bronze DB   : {BRONZE_DB}")
print(f"🗂  Catalog     : {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Librerías e Inicialización de Spark

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, lit, input_file_name, col
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType
)
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("BronzeLayer")

# Spark ya está disponible en Databricks como 'spark'
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
print("✅ Spark Session inicializada correctamente")
print(f"   Versión Spark: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Creación del Esquema Bronze

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {BRONZE_DB}
    COMMENT 'Capa Bronze: datos crudos en formato Delta, sin transformaciones de negocio'
""")
spark.sql(f"USE SCHEMA {BRONZE_DB}")

print(f"✅ Esquema '{CATALOG}.{BRONZE_DB}' verificado/creado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Definición del Mapa de Archivos Fuente

# COMMAND ----------

# Mapa: nombre_tabla -> archivo_csv en la capa Raw
SOURCE_FILES = {
    "orders":               "olist_orders_dataset.csv",
    "order_items":          "olist_order_items_dataset.csv",
    "order_payments":       "olist_order_payments_dataset.csv",
    "order_reviews":        "olist_order_reviews_dataset.csv",
    "customers":            "olist_customers_dataset.csv",
    "products":             "olist_products_dataset.csv",
    "sellers":              "olist_sellers_dataset.csv",
    "geolocation":          "olist_geolocation_dataset.csv",
    "product_category_translation": "product_category_name_translation.csv",
}

print("📋 Archivos a procesar:")
for table, file in SOURCE_FILES.items():
    print(f"   → {file:55s} → bronze.{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Función Principal de Ingesta a Bronze

# COMMAND ----------

def load_csv_to_bronze(table_name: str, csv_filename: str) -> dict:
    """
    Lee un CSV desde la capa Raw y lo escribe como Delta Table en Bronze.
    Agrega columnas de auditoría: _ingestion_timestamp, _source_file.

    Args:
        table_name   : Nombre de la tabla destino en Bronze
        csv_filename : Nombre del archivo CSV en la capa Raw

    Returns:
        dict con métricas de la carga
    """
    source_path = f"{RAW_PATH}/{csv_filename}"
    target_table = f"{CATALOG}.{BRONZE_DB}.{table_name}"

    logger.info(f"⬆️  Iniciando ingesta: {csv_filename} → {target_table}")

    try:
        # 1. Leer CSV con inferencia de esquema (Bronze no transforma tipos)
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")          # Infiere tipos básicos
              .option("multiLine", "true")            # Soporta campos con saltos de línea
              .option("escape", '"')
              .option("encoding", "UTF-8")
              .csv(source_path))

        # 2. Agregar columnas de auditoría (lineage y trazabilidad)
        df_bronze = (df
                     .withColumn("_ingestion_timestamp", current_timestamp())
                     .withColumn("_source_file", lit(csv_filename)))

        record_count = df_bronze.count()

        # 3. Escribir como Delta Table — modo overwrite para recargas idempotentes
        (df_bronze.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .option("delta.autoOptimize.optimizeWrite", "true")
         .option("delta.autoOptimize.autoCompact", "true")
         .saveAsTable(target_table))

        logger.info(f"✅ {target_table} — {record_count:,} registros cargados")
        return {"table": target_table, "records": record_count, "status": "SUCCESS"}

    except Exception as e:
        logger.error(f"❌ Error en {target_table}: {str(e)}")
        return {"table": target_table, "records": 0, "status": f"ERROR: {str(e)}"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ejecución de la Ingesta

# COMMAND ----------

results = []
print("=" * 70)
print("🚀 INICIANDO CARGA BRONZE LAYER")
print("=" * 70)

for table_name, csv_file in SOURCE_FILES.items():
    result = load_csv_to_bronze(table_name, csv_file)
    results.append(result)

print("\n" + "=" * 70)
print("📊 RESUMEN DE CARGA BRONZE")
print("=" * 70)

total_records = 0
errors = 0
for r in results:
    status_icon = "✅" if r["status"] == "SUCCESS" else "❌"
    print(f"{status_icon} {r['table'].split('.')[-1]:40s} | {r['records']:>10,} registros | {r['status']}")
    if r["status"] == "SUCCESS":
        total_records += r["records"]
    else:
        errors += 1

print("=" * 70)
print(f"📦 Total registros cargados : {total_records:,}")
print(f"✅ Tablas exitosas          : {len(results) - errors}")
print(f"❌ Tablas con error         : {errors}")

if errors > 0:
    raise Exception(f"Bronze Layer finalizó con {errors} error(es). Revisar logs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validación Post-Carga

# COMMAND ----------

print("🔍 Validación de tablas Bronze creadas:\n")
spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_DB}").show(20, truncate=False)

# COMMAND ----------

# Ejemplo de preview de la tabla más importante
print("📋 Preview — bronze_ecommerce.orders (primeras 5 filas):")
spark.sql(f"SELECT * FROM {CATALOG}.{BRONZE_DB}.orders LIMIT 5").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Registro de Ejecución en Log

# COMMAND ----------

import json
from datetime import datetime

log_data = {
    "layer": "bronze",
    "execution_timestamp": datetime.now().isoformat(),
    "pipeline": "olist_ecommerce_elt",
    "results": results,
    "total_records": total_records,
    "errors": errors,
    "status": "COMPLETED" if errors == 0 else "COMPLETED_WITH_ERRORS"
}

log_path = f"/Volumes/workspace/raw/olist_data/bronze_execution_log.json"
dbutils.fs.put(log_path, json.dumps(log_data, indent=2), overwrite=True)

print(f"📝 Log de ejecución guardado en: {log_path}")
print(f"\n🏁 Bronze Layer finalizado — Status: {log_data['status']}")
