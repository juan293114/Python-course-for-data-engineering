# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer — Limpieza, Estandarización y Calidad de Datos
# MAGIC ## Pipeline ELT | Arquitectura Medallón | Olist Brazilian E-Commerce
# MAGIC
# MAGIC **Objetivo:** Transformar los datos de la capa Bronze aplicando:
# MAGIC - Eliminación de duplicados
# MAGIC - Normalización de tipos de datos (fechas, numéricos, strings)
# MAGIC - Manejo de valores nulos con reglas de negocio
# MAGIC - Estandarización de categorías y textos
# MAGIC - Validaciones y métricas de calidad de datos

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración de Parámetros

# COMMAND ----------

dbutils.widgets.text("bronze_schema", "bronze_ecommerce", "Bronze Schema")
dbutils.widgets.text("silver_schema", "silver_ecommerce", "Silver Schema")
dbutils.widgets.text("catalog", "workspace", "Catalog")

BRONZE_DB = dbutils.widgets.get("bronze_schema")
SILVER_DB  = dbutils.widgets.get("silver_schema")
CATALOG    = dbutils.widgets.get("catalog")

BRONZE_FULL = f"{CATALOG}.{BRONZE_DB}"
SILVER_FULL = f"{CATALOG}.{SILVER_DB}"

print(f"🥉 Bronze : {BRONZE_FULL}")
print(f"🥈 Silver : {SILVER_FULL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Librerías e Inicialización

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, upper, lower, initcap,
    to_timestamp, to_date, date_format,
    when, coalesce, lit, regexp_replace,
    round as spark_round, abs as spark_abs,
    current_timestamp, count, sum as spark_sum,
    isnan, isnull, datediff, year, month, dayofmonth,
    monotonically_increasing_id, sha2, concat_ws
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, TimestampType, DateType
)
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SilverLayer")

print("✅ Librerías cargadas correctamente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Creación del Esquema Silver

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_DB}")
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {SILVER_DB}
    COMMENT 'Capa Silver: datos limpios, estandarizados y validados para transformaciones de negocio'
""")
spark.sql(f"USE SCHEMA {SILVER_DB}")
print(f"✅ Esquema '{SILVER_FULL}' verificado/creado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Funciones Auxiliares de Calidad de Datos

# COMMAND ----------

def log_quality_report(df: DataFrame, table_name: str, key_cols: list = None):
    """Genera un reporte de calidad de datos para una tabla."""
    record_count = df.count()
    print(f"\n{'='*60}")
    print(f"📊 REPORTE DE CALIDAD — {table_name}")
    print(f"{'='*60}")
    print(f"   Total registros: {record_count:,}")

    # Nulos por columna — separar numéricos de strings
    numeric_types = ('double', 'float')
    null_exprs = []
    for c in df.columns:
        type_name = df.schema[c].dataType.typeName()
        if type_name in numeric_types:
            # Para numéricos: chequear null E isnan
            null_exprs.append(
                spark_sum(when(col(c).isNull() | isnan(col(c)), 1).otherwise(0)).alias(c)
            )
        elif type_name in ('string', 'integer', 'long', 'timestamp'):
            # Para strings y otros: solo chequear null
            null_exprs.append(
                spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
            )

    if null_exprs:
        null_counts = df.select(null_exprs)
        null_df = null_counts.collect()[0].asDict()
        cols_with_nulls = {k: v for k, v in null_df.items() if v and v > 0}
        if cols_with_nulls:
            print(f"   ⚠️  Columnas con nulos:")
            for col_name, null_cnt in cols_with_nulls.items():
                pct = (null_cnt / record_count * 100) if record_count > 0 else 0
                print(f"      - {col_name}: {null_cnt:,} ({pct:.1f}%)")
        else:
            print("   ✅ Sin valores nulos en columnas críticas")

    # Duplicados
    if key_cols:
        dup_count = record_count - df.dropDuplicates(key_cols).count()
        print(f"   {'⚠️' if dup_count > 0 else '✅'} Duplicados por {key_cols}: {dup_count:,}")

    print(f"{'='*60}\n")
    return record_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transformación — silver_orders

# COMMAND ----------

print("🔄 Procesando: silver_orders")

df_orders_raw = spark.table(f"{BRONZE_FULL}.orders")

df_silver_orders = (
    df_orders_raw
    # Deduplicar por clave primaria
    .dropDuplicates(["order_id"])

    # Normalizar tipos de fecha
    .withColumn("order_purchase_timestamp",
                to_timestamp(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("order_approved_at",
                to_timestamp(col("order_approved_at"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("order_delivered_carrier_date",
                to_timestamp(col("order_delivered_carrier_date"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("order_delivered_customer_date",
                to_timestamp(col("order_delivered_customer_date"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("order_estimated_delivery_date",
                to_timestamp(col("order_estimated_delivery_date"), "yyyy-MM-dd HH:mm:ss"))

    # Estandarizar status
    .withColumn("order_status", upper(trim(col("order_status"))))

    # Filtrar registros sin ID de orden (datos inválidos)
    .filter(col("order_id").isNotNull() & col("customer_id").isNotNull())

    # Calcular días de entrega (métrica de negocio)
    .withColumn("delivery_days",
                when(col("order_delivered_customer_date").isNotNull(),
                     datediff(col("order_delivered_customer_date"),
                              col("order_purchase_timestamp")))
                .otherwise(None))

    # Calcular retraso respecto a estimado (positivo = tardó más de lo estimado)
    .withColumn("delivery_delay_days",
                when(col("order_delivered_customer_date").isNotNull() &
                     col("order_estimated_delivery_date").isNotNull(),
                     datediff(col("order_delivered_customer_date"),
                              col("order_estimated_delivery_date")))
                .otherwise(None))

    # Columna de auditoría
    .withColumn("_silver_timestamp", current_timestamp())

    # Seleccionar columnas finales
    .select(
        "order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "delivery_days", "delivery_delay_days",
        "_silver_timestamp"
    )
)

log_quality_report(df_silver_orders, "silver_orders", ["order_id"])
save_silver_table(df_silver_orders, "silver_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Transformación — silver_order_items

# COMMAND ----------

print("🔄 Procesando: silver_order_items")

df_items_raw = spark.table(f"{BRONZE_FULL}.order_items")

df_silver_items = (
    df_items_raw
    .dropDuplicates(["order_id", "order_item_id"])

    # Normalizar fecha de envío
    .withColumn("shipping_limit_date",
                to_timestamp(col("shipping_limit_date"), "yyyy-MM-dd HH:mm:ss"))

    # Convertir a Double y asegurar valores positivos
    .withColumn("price",
                when(col("price").cast(DoubleType()) >= 0,
                     col("price").cast(DoubleType()))
                .otherwise(None))
    .withColumn("freight_value",
                when(col("freight_value").cast(DoubleType()) >= 0,
                     col("freight_value").cast(DoubleType()))
                .otherwise(lit(0.0)))

    # Calcular valor total del ítem
    .withColumn("total_item_value",
                spark_round(
                    coalesce(col("price"), lit(0.0)) +
                    coalesce(col("freight_value"), lit(0.0)),
                    2))

    # Filtrar registros sin IDs críticos
    .filter(col("order_id").isNotNull() & col("product_id").isNotNull())

    .withColumn("_silver_timestamp", current_timestamp())

    .select(
        "order_id", "order_item_id", "product_id", "seller_id",
        "shipping_limit_date", "price", "freight_value",
        "total_item_value", "_silver_timestamp"
    )
)

log_quality_report(df_silver_items, "silver_order_items", ["order_id", "order_item_id"])
save_silver_table(df_silver_items, "silver_order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Transformación — silver_order_payments

# COMMAND ----------

print("🔄 Procesando: silver_order_payments")

df_payments_raw = spark.table(f"{BRONZE_FULL}.order_payments")

df_silver_payments = (
    df_payments_raw
    .dropDuplicates(["order_id", "payment_sequential"])

    .withColumn("payment_type",
                when(col("payment_type").isNull(), lit("unknown"))
                .otherwise(lower(trim(col("payment_type")))))

    .withColumn("payment_installments",
                col("payment_installments").cast(IntegerType()))

    .withColumn("payment_value",
                spark_round(col("payment_value").cast(DoubleType()), 2))

    # Filtrar pagos negativos o nulos
    .filter(col("payment_value") > 0)
    .filter(col("order_id").isNotNull())

    .withColumn("_silver_timestamp", current_timestamp())

    .select(
        "order_id", "payment_sequential", "payment_type",
        "payment_installments", "payment_value", "_silver_timestamp"
    )
)

log_quality_report(df_silver_payments, "silver_order_payments", ["order_id", "payment_sequential"])
save_silver_table(df_silver_payments, "silver_order_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Transformación — silver_order_reviews

# COMMAND ----------

print("🔄 Procesando: silver_order_reviews")

df_reviews_raw = spark.table(f"{BRONZE_FULL}.order_reviews")

df_silver_reviews = (
    df_reviews_raw
    .dropDuplicates(["review_id"])

    .withColumn("review_creation_date",
                to_timestamp(col("review_creation_date"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("review_answer_timestamp",
                to_timestamp(col("review_answer_timestamp"), "yyyy-MM-dd HH:mm:ss"))

    .withColumn("review_score",
                col("review_score").cast(IntegerType()))

    # Validar score entre 1 y 5
    .filter(col("review_score").between(1, 5))

    # Limpiar texto de comentarios
    .withColumn("review_comment_title",
                when(col("review_comment_title").isNotNull(),
                     trim(col("review_comment_title")))
                .otherwise(lit(None)))
    .withColumn("review_comment_message",
                when(col("review_comment_message").isNotNull(),
                     trim(col("review_comment_message")))
                .otherwise(lit(None)))

    # Clasificar satisfacción
    .withColumn("satisfaction_level",
                when(col("review_score") >= 4, lit("SATISFECHO"))
                .when(col("review_score") == 3, lit("NEUTRO"))
                .otherwise(lit("INSATISFECHO")))

    .withColumn("_silver_timestamp", current_timestamp())

    .select(
        "review_id", "order_id", "review_score",
        "review_comment_title", "review_comment_message",
        "review_creation_date", "review_answer_timestamp",
        "satisfaction_level", "_silver_timestamp"
    )
)

log_quality_report(df_silver_reviews, "silver_order_reviews", ["review_id"])
save_silver_table(df_silver_reviews, "silver_order_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Transformación — silver_customers

# COMMAND ----------

print("🔄 Procesando: silver_customers")

df_customers_raw = spark.table(f"{BRONZE_FULL}.customers")

df_silver_customers = (
    df_customers_raw
    .dropDuplicates(["customer_id"])

    .withColumn("customer_city",
                initcap(trim(lower(col("customer_city")))))
    .withColumn("customer_state",
                upper(trim(col("customer_state"))))
    .withColumn("customer_zip_code_prefix",
                col("customer_zip_code_prefix").cast(StringType()))

    .filter(col("customer_id").isNotNull())

    .withColumn("_silver_timestamp", current_timestamp())

    .select(
        "customer_id", "customer_unique_id",
        "customer_zip_code_prefix", "customer_city",
        "customer_state", "_silver_timestamp"
    )
)

log_quality_report(df_silver_customers, "silver_customers", ["customer_id"])
save_silver_table(df_silver_customers, "silver_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Transformación — silver_products

# COMMAND ----------

print("🔄 Procesando: silver_products")

df_products_raw   = spark.table(f"{BRONZE_FULL}.products")
df_category_trans = spark.table(f"{BRONZE_FULL}.product_category_translation")

df_silver_products = (
    df_products_raw
    .dropDuplicates(["product_id"])

    # JOIN para agregar categoría en inglés
    .join(df_category_trans,
          on="product_category_name",
          how="left")

    .withColumn("product_category_name",
                when(col("product_category_name").isNull(), lit("unknown"))
                .otherwise(lower(trim(col("product_category_name")))))

    .withColumn("product_category_name_english",
                when(col("product_category_name_english").isNull(), lit("unknown"))
                .otherwise(lower(trim(col("product_category_name_english")))))

    # Normalizar dimensiones y pesos
    .withColumn("product_weight_g",
                col("product_weight_g").cast(DoubleType()))
    .withColumn("product_length_cm",
                col("product_length_cm").cast(DoubleType()))
    .withColumn("product_height_cm",
                col("product_height_cm").cast(DoubleType()))
    .withColumn("product_width_cm",
                col("product_width_cm").cast(DoubleType()))

    # Calcular volumen del producto (cm³)
    .withColumn("product_volume_cm3",
                when(col("product_length_cm").isNotNull() &
                     col("product_height_cm").isNotNull() &
                     col("product_width_cm").isNotNull(),
                     spark_round(
                         col("product_length_cm") *
                         col("product_height_cm") *
                         col("product_width_cm"), 2))
                .otherwise(None))

    .filter(col("product_id").isNotNull())

    .withColumn("_silver_timestamp", current_timestamp())

    .select(
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_volume_cm3",
        "_silver_timestamp"
    )
)

log_quality_report(df_silver_products, "silver_products", ["product_id"])
save_silver_table(df_silver_products, "silver_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Transformación — silver_sellers

# COMMAND ----------

print("🔄 Procesando: silver_sellers")

df_sellers_raw = spark.table(f"{BRONZE_FULL}.sellers")

df_silver_sellers = (
    df_sellers_raw
    .dropDuplicates(["seller_id"])

    .withColumn("seller_city",
                initcap(trim(lower(col("seller_city")))))
    .withColumn("seller_state",
                upper(trim(col("seller_state"))))
    .withColumn("seller_zip_code_prefix",
                col("seller_zip_code_prefix").cast(StringType()))

    .filter(col("seller_id").isNotNull())

    .withColumn("_silver_timestamp", current_timestamp())

    .select(
        "seller_id", "seller_zip_code_prefix",
        "seller_city", "seller_state",
        "_silver_timestamp"
    )
)

log_quality_report(df_silver_sellers, "silver_sellers", ["seller_id"])
save_silver_table(df_silver_sellers, "silver_sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Resumen Final Silver Layer

# COMMAND ----------

silver_tables = [
    "silver_orders", "silver_order_items", "silver_order_payments",
    "silver_order_reviews", "silver_customers", "silver_products", "silver_sellers"
]

print("\n" + "="*70)
print("📊 RESUMEN SILVER LAYER")
print("="*70)

total = 0
for tbl in silver_tables:
    count = spark.table(f"{SILVER_FULL}.{tbl}").count()
    total += count
    print(f"   ✅ {tbl:40s} | {count:>10,} registros")

print("="*70)
print(f"   📦 Total registros Silver: {total:,}")
print(f"   🕐 Timestamp: {datetime.now().isoformat()}")
print("="*70)
print("\n🏁 Silver Layer completado exitosamente")
