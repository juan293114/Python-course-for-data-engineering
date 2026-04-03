# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer — Modelo Estrella para Análisis de E-Commerce
# MAGIC ## Pipeline ELT | Arquitectura Medallón | Olist Brazilian E-Commerce
# MAGIC
# MAGIC **Objetivo:** Construir el modelo dimensional (Star Schema) listo para consumo en Power BI.
# MAGIC
# MAGIC ### Modelo Estrella diseñado:
# MAGIC
# MAGIC ```
# MAGIC                    ┌─────────────────┐
# MAGIC                    │   dim_date      │
# MAGIC                    │  (fecha compra) │
# MAGIC                    └────────┬────────┘
# MAGIC                             │
# MAGIC  ┌──────────────┐   ┌───────┴────────┐   ┌──────────────────┐
# MAGIC  │ dim_customer │───│ fact_order_    │───│   dim_product    │
# MAGIC  │  (cliente)   │   │   items        │   │   (producto)     │
# MAGIC  └──────────────┘   │  (TABLA DE     │   └──────────────────┘
# MAGIC                     │   HECHOS)      │
# MAGIC  ┌──────────────┐   │               │   ┌──────────────────┐
# MAGIC  │  dim_seller  │───│               │───│  dim_payment     │
# MAGIC  │ (vendedor)   │   └───────────────┘   │ (método de pago) │
# MAGIC  └──────────────┘                       └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Tabla de Hechos:** `fact_order_items`
# MAGIC   - Granularidad: un registro por ítem de pedido
# MAGIC   - Métricas: precio, flete, valor total, días de entrega, retraso, score de review
# MAGIC
# MAGIC **Dimensiones:**
# MAGIC   - `dim_customer`: datos del cliente (ciudad, estado, región)
# MAGIC   - `dim_product`: datos del producto (categoría, peso, volumen)
# MAGIC   - `dim_seller`: datos del vendedor (ciudad, estado)
# MAGIC   - `dim_date`: calendario completo (año, mes, trimestre, día semana)
# MAGIC   - `dim_payment`: método y cuotas de pago

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración de Parámetros

# COMMAND ----------

dbutils.widgets.text("silver_schema", "silver_ecommerce", "Silver Schema")
dbutils.widgets.text("gold_schema",   "gold_ecommerce",   "Gold Schema")
dbutils.widgets.text("catalog",       "workspace",        "Catalog")

SILVER_DB = dbutils.widgets.get("silver_schema")
GOLD_DB   = dbutils.widgets.get("gold_schema")
CATALOG   = dbutils.widgets.get("catalog")

SILVER_FULL = f"{CATALOG}.{SILVER_DB}"
GOLD_FULL   = f"{CATALOG}.{GOLD_DB}"

print(f"🥈 Silver : {SILVER_FULL}")
print(f"🥇 Gold   : {GOLD_FULL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Librerías

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, upper, lower, trim, initcap,
    year, month, quarter, dayofmonth, dayofweek, weekofyear,
    date_format, to_date, to_timestamp, current_timestamp,
    round as spark_round, avg, sum as spark_sum, count,
    min as spark_min, max as spark_max,
    monotonically_increasing_id, sha2, concat_ws, row_number,
    dense_rank
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    IntegerType, DoubleType, StringType, DateType, LongType
)
import logging
from datetime import datetime

logger = logging.getLogger("GoldLayer")
print("✅ Librerías cargadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Creación del Esquema Gold

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_DB}")
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {GOLD_DB}
    COMMENT 'Capa Gold: Modelo Estrella para análisis de E-Commerce Olist. Optimizado para Power BI.'
""")
spark.sql(f"USE SCHEMA {GOLD_DB}")
print(f"✅ Esquema '{GOLD_FULL}' verificado/creado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Función Auxiliar para Guardar Tablas Gold

# COMMAND ----------

def save_gold_table(df: DataFrame, table_name: str, partition_col: str = None):
    """Escribe una tabla Delta en la capa Gold."""
    target = f"{GOLD_FULL}.{table_name}"
    writer = (df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("delta.autoOptimize.optimizeWrite", "true")
                .option("delta.autoOptimize.autoCompact", "true"))

    if partition_col:
        writer = writer.partitionBy(partition_col)

    writer.saveAsTable(target)
    count = spark.table(target).count()
    logger.info(f"✅ {target} — {count:,} registros")
    print(f"   ✅ {table_name:40s} | {count:>10,} registros")
    return count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. DIMENSIÓN — dim_date (Calendario)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Generar tabla de fechas completa para el rango del dataset

# COMMAND ----------

print("🔄 Construyendo dim_date...")

# Obtener rango de fechas desde los pedidos reales
df_orders_dates = spark.table(f"{SILVER_FULL}.silver_orders").select(
    spark_min(to_date("order_purchase_timestamp")).alias("min_date"),
    spark_max(to_date("order_purchase_timestamp")).alias("max_date")
).collect()[0]

min_date = df_orders_dates["min_date"]
max_date = df_orders_dates["max_date"]

print(f"   Rango de fechas del dataset: {min_date} → {max_date}")

# Generar secuencia de fechas con SQL
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW date_range AS
    SELECT explode(sequence(
        to_date('{min_date}'),
        to_date('{max_date}'),
        interval 1 day
    )) AS date
""")

df_dim_date = (
    spark.table("date_range")
    .withColumn("date_key",
                (year("date") * 10000 +
                 month("date") * 100 +
                 dayofmonth("date")).cast(IntegerType()))
    .withColumn("year",          year("date").cast(IntegerType()))
    .withColumn("quarter",       quarter("date").cast(IntegerType()))
    .withColumn("quarter_label", concat_ws("", lit("Q"), quarter("date").cast(StringType())))
    .withColumn("month",         month("date").cast(IntegerType()))
    .withColumn("month_name",    date_format("date", "MMMM"))
    .withColumn("month_abbr",    date_format("date", "MMM"))
    .withColumn("day",           dayofmonth("date").cast(IntegerType()))
    .withColumn("day_of_week",   dayofweek("date").cast(IntegerType()))      # 1=Dom, 7=Sab
    .withColumn("day_name",      date_format("date", "EEEE"))
    .withColumn("week_of_year",  weekofyear("date").cast(IntegerType()))
    .withColumn("is_weekend",
                when(dayofweek("date").isin([1, 7]), lit(True))
                .otherwise(lit(False)))
    .withColumn("semester",
                when(month("date") <= 6, lit(1)).otherwise(lit(2)).cast(IntegerType()))
    .withColumn("year_month",    date_format("date", "yyyy-MM"))
    .withColumn("_gold_timestamp", current_timestamp())
    .select(
        "date_key", "date", "year", "semester", "quarter", "quarter_label",
        "month", "month_name", "month_abbr", "week_of_year",
        "day", "day_of_week", "day_name", "is_weekend",
        "year_month", "_gold_timestamp"
    )
)

save_gold_table(df_dim_date, "dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. DIMENSIÓN — dim_customer (Clientes)

# COMMAND ----------

print("🔄 Construyendo dim_customer...")

df_silver_customers = spark.table(f"{SILVER_FULL}.silver_customers")

# Mapa de estados brasileños a región geográfica
region_map = {
    "SP": "Sudeste", "RJ": "Sudeste", "MG": "Sudeste", "ES": "Sudeste",
    "RS": "Sul",     "SC": "Sul",     "PR": "Sul",
    "BA": "Nordeste", "PE": "Nordeste", "CE": "Nordeste", "MA": "Nordeste",
    "PB": "Nordeste", "RN": "Nordeste", "AL": "Nordeste", "SE": "Nordeste",
    "PI": "Nordeste",
    "AM": "Norte",   "PA": "Norte",   "AC": "Norte",   "RO": "Norte",
    "RR": "Norte",   "AP": "Norte",   "TO": "Norte",
    "MT": "Centro-Oeste", "MS": "Centro-Oeste", "GO": "Centro-Oeste", "DF": "Centro-Oeste"
}

# Crear función para mapear región (via join con tabla temporal)
region_data = [(k, v) for k, v in region_map.items()]
df_regions = spark.createDataFrame(region_data, ["customer_state", "region"])

df_dim_customer = (
    df_silver_customers
    .join(df_regions, on="customer_state", how="left")

    # Surrogate Key (entero secuencial)
    .withColumn("customer_key", monotonically_increasing_id().cast(LongType()))

    .withColumn("region",
                coalesce(col("region"), lit("Outro")))

    .withColumn("_gold_timestamp", current_timestamp())

    .select(
        "customer_key",
        "customer_id",                # Natural Key (para joins con la fact table)
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "region",
        "_gold_timestamp"
    )
)

save_gold_table(df_dim_customer, "dim_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. DIMENSIÓN — dim_product (Productos)

# COMMAND ----------

print("🔄 Construyendo dim_product...")

df_silver_products = spark.table(f"{SILVER_FULL}.silver_products")

df_dim_product = (
    df_silver_products
    .withColumn("product_key", monotonically_increasing_id().cast(LongType()))

    # Clasificar tamaño del producto por peso
    .withColumn("product_size_category",
                when(col("product_weight_g") < 500,   lit("Pequeno"))
                .when(col("product_weight_g") < 2000,  lit("Mediano"))
                .when(col("product_weight_g") < 10000, lit("Grande"))
                .otherwise(lit("Extra Grande")))

    .withColumn("_gold_timestamp", current_timestamp())

    .select(
        "product_key",
        "product_id",                           # Natural Key
        "product_category_name",
        "product_category_name_english",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_volume_cm3",
        "product_photos_qty",
        "product_size_category",
        "_gold_timestamp"
    )
)

save_gold_table(df_dim_product, "dim_product")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. DIMENSIÓN — dim_seller (Vendedores)

# COMMAND ----------

print("🔄 Construyendo dim_seller...")

df_silver_sellers = spark.table(f"{SILVER_FULL}.silver_sellers")

# Reutilizar mapa de regiones para sellers
region_data_s = [(k, v) for k, v in region_map.items()]
df_regions_s = spark.createDataFrame(region_data_s, ["seller_state", "seller_region"])

df_dim_seller = (
    df_silver_sellers
    .join(df_regions_s, on="seller_state", how="left")

    .withColumn("seller_key", monotonically_increasing_id().cast(LongType()))

    .withColumn("seller_region",
                coalesce(col("seller_region"), lit("Outro")))

    .withColumn("_gold_timestamp", current_timestamp())

    .select(
        "seller_key",
        "seller_id",            # Natural Key
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "seller_region",
        "_gold_timestamp"
    )
)

save_gold_table(df_dim_seller, "dim_seller")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. DIMENSIÓN — dim_payment (Métodos de Pago)

# COMMAND ----------

print("🔄 Construyendo dim_payment...")

df_silver_payments = spark.table(f"{SILVER_FULL}.silver_order_payments")

df_dim_payment = (
    df_silver_payments
    .select(
        col("payment_type").alias("payment_type"),
        col("payment_installments").alias("payment_installments")
    )
    .distinct()

    # Clasificar cuotas
    .withColumn("installment_category",
                when(col("payment_installments") == 1,        lit("Pago único"))
                .when(col("payment_installments").between(2, 6),  lit("Cuotas cortas (2-6)"))
                .when(col("payment_installments").between(7, 12), lit("Cuotas medias (7-12)"))
                .otherwise(lit("Cuotas largas (>12)")))

    # Etiqueta legible del tipo de pago
    .withColumn("payment_type_label",
                when(col("payment_type") == "credit_card",  lit("Tarjeta de Crédito"))
                .when(col("payment_type") == "boleto",       lit("Boleto Bancário"))
                .when(col("payment_type") == "voucher",      lit("Voucher"))
                .when(col("payment_type") == "debit_card",   lit("Tarjeta de Débito"))
                .otherwise(col("payment_type")))

    .withColumn("payment_key", monotonically_increasing_id().cast(LongType()))
    .withColumn("_gold_timestamp", current_timestamp())

    .select(
        "payment_key",
        "payment_type",
        "payment_type_label",
        "payment_installments",
        "installment_category",
        "_gold_timestamp"
    )
)

save_gold_table(df_dim_payment, "dim_payment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. TABLA DE HECHOS — fact_order_items

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1 Construir la Fact Table con JOINs hacia las dimensiones

# COMMAND ----------

print("🔄 Construyendo fact_order_items...")

# Cargar dimensiones ya creadas (para resolver surrogate keys)
dim_customer = spark.table(f"{GOLD_FULL}.dim_customer").select("customer_key", "customer_id")
dim_product  = spark.table(f"{GOLD_FULL}.dim_product").select("product_key", "product_id")
dim_seller   = spark.table(f"{GOLD_FULL}.dim_seller").select("seller_key", "seller_id")
dim_date     = spark.table(f"{GOLD_FULL}.dim_date").select("date_key", "date")
dim_payment  = spark.table(f"{GOLD_FULL}.dim_payment").select("payment_key", "payment_type", "payment_installments")

# Cargar tablas Silver base
silver_items    = spark.table(f"{SILVER_FULL}.silver_order_items")
silver_orders   = spark.table(f"{SILVER_FULL}.silver_orders")
silver_payments = spark.table(f"{SILVER_FULL}.silver_order_payments")
silver_reviews  = spark.table(f"{SILVER_FULL}.silver_order_reviews")
silver_customers = spark.table(f"{SILVER_FULL}.silver_customers")

# COMMAND ----------

# Agregar pagos por orden (resumir a nivel de orden para el join)
df_payments_agg = (
    silver_payments
    .groupBy("order_id")
    .agg(
        spark_sum("payment_value").alias("total_payment_value"),
        spark_min("payment_type").alias("primary_payment_type"),          # Tipo de pago principal
        spark_min("payment_installments").alias("payment_installments"),
        count("payment_sequential").alias("payment_count")
    )
)

# Agregar review por orden (puede haber múltiples, tomar el score promedio)
df_reviews_agg = (
    silver_reviews
    .groupBy("order_id")
    .agg(
        avg("review_score").alias("avg_review_score"),
        count("review_id").alias("review_count")
    )
    .withColumn("avg_review_score", spark_round(col("avg_review_score"), 2))
)

# COMMAND ----------

# Construcción de la Fact Table
df_fact = (
    # Base: ítems de orden (granularidad del fact)
    silver_items
    .alias("items")

    # JOIN con órdenes (para fechas, estado, customer_id)
    .join(silver_orders.alias("orders"),
          on="order_id", how="inner")

    # JOIN con clientes (para resolver customer_key)
    .join(silver_customers.alias("cust"),
          on="customer_id", how="left")

    # JOIN con pagos agregados
    .join(df_payments_agg.alias("pay"),
          on="order_id", how="left")

    # JOIN con reviews
    .join(df_reviews_agg.alias("rev"),
          on="order_id", how="left")

    # ── Resolución de Surrogate Keys ──

    # customer_key
    .join(dim_customer.alias("dc"),
          col("cust.customer_id") == col("dc.customer_id"), how="left")

    # product_key
    .join(dim_product.alias("dp"),
          col("items.product_id") == col("dp.product_id"), how="left")

    # seller_key
    .join(dim_seller.alias("ds"),
          col("items.seller_id") == col("ds.seller_id"), how="left")

    # date_key — usar fecha de compra
    .withColumn("purchase_date", to_date(col("orders.order_purchase_timestamp")))
    .join(dim_date.alias("dd"),
          col("purchase_date") == col("dd.date"), how="left")

    # payment_key
    .join(dim_payment.alias("dpay"),
          (col("pay.primary_payment_type") == col("dpay.payment_type")) &
          (col("pay.payment_installments") == col("dpay.payment_installments")),
          how="left")

    # ── Selección de columnas finales del Fact ──
    .select(
        # Surrogate Keys (claves foráneas al modelo estrella)
        col("dc.customer_key").alias("customer_key"),
        col("dp.product_key").alias("product_key"),
        col("ds.seller_key").alias("seller_key"),
        col("dd.date_key").alias("date_key"),
        col("dpay.payment_key").alias("payment_key"),

        # Natural Keys (para trazabilidad y debugging)
        col("items.order_id"),
        col("items.order_item_id"),
        col("items.product_id"),
        col("items.seller_id"),
        col("cust.customer_id"),

        # Medidas (métricas del negocio)
        col("items.price").alias("item_price"),
        col("items.freight_value").alias("freight_value"),
        col("items.total_item_value"),
        col("pay.total_payment_value"),
        col("pay.payment_count"),

        # Medidas de entrega
        col("orders.order_status"),
        col("orders.delivery_days"),
        col("orders.delivery_delay_days"),

        # Medidas de satisfacción
        col("rev.avg_review_score").alias("review_score"),

        # Fechas desnormalizadas (útiles para slicing en Power BI)
        col("orders.order_purchase_timestamp"),
        col("orders.order_delivered_customer_date"),

        # Auditoría
        current_timestamp().alias("_gold_timestamp")
    )
)

save_gold_table(df_fact, "fact_order_items", partition_col=None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Vistas de Análisis Pre-computadas (Agregaciones para Power BI)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.1 Vista — Ventas por Categoría de Producto y Mes

# COMMAND ----------

print("🔄 Creando vista: vw_ventas_por_categoria_mes...")

spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_FULL}.vw_ventas_por_categoria_mes AS
    SELECT
        d.year,
        d.month,
        d.month_name,
        d.year_month,
        p.product_category_name_english     AS category_english,
        p.product_category_name             AS category_spanish,
        COUNT(DISTINCT f.order_id)          AS total_orders,
        COUNT(f.order_item_id)              AS total_items,
        ROUND(SUM(f.item_price), 2)         AS total_revenue,
        ROUND(SUM(f.freight_value), 2)      AS total_freight,
        ROUND(AVG(f.item_price), 2)         AS avg_item_price,
        ROUND(AVG(f.review_score), 2)       AS avg_review_score
    FROM {GOLD_FULL}.fact_order_items f
    JOIN {GOLD_FULL}.dim_date d     ON f.date_key    = d.date_key
    JOIN {GOLD_FULL}.dim_product p  ON f.product_key = p.product_key
    WHERE f.order_status = 'DELIVERED'
    GROUP BY 1,2,3,4,5,6
    ORDER BY d.year, d.month, total_revenue DESC
""")
print("   ✅ vw_ventas_por_categoria_mes creada")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.2 Vista — Performance de Vendedores

# COMMAND ----------

print("🔄 Creando vista: vw_performance_vendedores...")

spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_FULL}.vw_performance_vendedores AS
    SELECT
        s.seller_id,
        s.seller_city,
        s.seller_state,
        s.seller_region,
        COUNT(DISTINCT f.order_id)          AS total_orders,
        COUNT(f.order_item_id)              AS total_items_sold,
        ROUND(SUM(f.item_price), 2)         AS total_revenue,
        ROUND(AVG(f.item_price), 2)         AS avg_price,
        ROUND(AVG(f.review_score), 2)       AS avg_review_score,
        ROUND(AVG(f.delivery_days), 1)      AS avg_delivery_days,
        SUM(CASE WHEN f.delivery_delay_days > 0 THEN 1 ELSE 0 END) AS late_deliveries,
        ROUND(
            SUM(CASE WHEN f.delivery_delay_days > 0 THEN 1 ELSE 0 END) * 100.0
            / COUNT(f.order_id), 1
        )                                   AS pct_late_deliveries
    FROM {GOLD_FULL}.fact_order_items f
    JOIN {GOLD_FULL}.dim_seller s ON f.seller_key = s.seller_key
    WHERE f.order_status = 'DELIVERED'
    GROUP BY 1,2,3,4
    ORDER BY total_revenue DESC
""")
print("   ✅ vw_performance_vendedores creada")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.3 Vista — Análisis por Método de Pago

# COMMAND ----------

print("🔄 Creando vista: vw_analisis_pagos...")

spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_FULL}.vw_analisis_pagos AS
    SELECT
        py.payment_type_label,
        py.installment_category,
        d.year,
        d.quarter_label,
        COUNT(DISTINCT f.order_id)          AS total_orders,
        ROUND(SUM(f.total_payment_value), 2) AS total_paid,
        ROUND(AVG(f.total_payment_value), 2) AS avg_order_value
    FROM {GOLD_FULL}.fact_order_items f
    JOIN {GOLD_FULL}.dim_payment py ON f.payment_key = py.payment_key
    JOIN {GOLD_FULL}.dim_date d     ON f.date_key    = d.date_key
    GROUP BY 1,2,3,4
    ORDER BY total_paid DESC
""")
print("   ✅ vw_analisis_pagos creada")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.4 Vista — Satisfacción por Estado del Cliente

# COMMAND ----------

print("🔄 Creando vista: vw_satisfaccion_por_estado...")

spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_FULL}.vw_satisfaccion_por_estado AS
    SELECT
        c.customer_state,
        c.region,
        COUNT(DISTINCT f.order_id)                              AS total_orders,
        ROUND(AVG(f.review_score), 2)                           AS avg_review_score,
        ROUND(AVG(f.delivery_days), 1)                          AS avg_delivery_days,
        SUM(CASE WHEN f.review_score >= 4 THEN 1 ELSE 0 END)   AS satisfied_orders,
        ROUND(
            SUM(CASE WHEN f.review_score >= 4 THEN 1 ELSE 0 END) * 100.0
            / COUNT(f.order_id), 1
        )                                                        AS satisfaction_rate_pct
    FROM {GOLD_FULL}.fact_order_items f
    JOIN {GOLD_FULL}.dim_customer c ON f.customer_key = c.customer_key
    WHERE f.review_score IS NOT NULL
    GROUP BY 1,2
    ORDER BY avg_review_score DESC
""")
print("   ✅ vw_satisfaccion_por_estado creada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Validación y Pruebas del Modelo Estrella

# COMMAND ----------

print("\n" + "="*70)
print("🔍 VALIDACIÓN DEL MODELO ESTRELLA — GOLD LAYER")
print("="*70)

# Test 1: Conteo de filas en todas las tablas
gold_tables = {
    "DIMENSIÓN":  ["dim_date", "dim_customer", "dim_product", "dim_seller", "dim_payment"],
    "FACT TABLE": ["fact_order_items"],
    "VISTAS":     ["vw_ventas_por_categoria_mes", "vw_performance_vendedores",
                   "vw_analisis_pagos", "vw_satisfaccion_por_estado"]
}

total_records = 0
for category, tables in gold_tables.items():
    print(f"\n   📂 {category}")
    for tbl in tables:
        try:
            cnt = spark.table(f"{GOLD_FULL}.{tbl}").count()
            total_records += cnt
            print(f"      ✅ {tbl:45s} | {cnt:>10,} filas")
        except Exception as e:
            print(f"      ❌ {tbl:45s} | ERROR: {e}")

print(f"\n   {'='*65}")
print(f"   📦 Total registros Gold: {total_records:,}")

# COMMAND ----------

# Test 2: Verificar integridad referencial fact → dim
print("\n🔍 Test de integridad referencial:")

fact = spark.table(f"{GOLD_FULL}.fact_order_items")
total_fact = fact.count()

# Claves sin match en dimensiones
orphan_customers = fact.filter(col("customer_key").isNull()).count()
orphan_products  = fact.filter(col("product_key").isNull()).count()
orphan_sellers   = fact.filter(col("seller_key").isNull()).count()
orphan_dates     = fact.filter(col("date_key").isNull()).count()

print(f"   Total registros en fact_order_items: {total_fact:,}")
print(f"   ⚠️  Sin customer_key: {orphan_customers:,} ({orphan_customers/total_fact*100:.2f}%)")
print(f"   ⚠️  Sin product_key : {orphan_products:,}  ({orphan_products/total_fact*100:.2f}%)")
print(f"   ⚠️  Sin seller_key  : {orphan_sellers:,}   ({orphan_sellers/total_fact*100:.2f}%)")
print(f"   ⚠️  Sin date_key    : {orphan_dates:,}     ({orphan_dates/total_fact*100:.2f}%)")

# COMMAND ----------

# Test 3: Métricas de negocio de muestra
print("\n📊 KPIs de Negocio (muestra de la capa Gold):")
spark.sql(f"""
    SELECT
        COUNT(DISTINCT order_id)            AS total_ordenes,
        ROUND(SUM(item_price), 2)           AS ingresos_totales_BRL,
        ROUND(AVG(item_price), 2)           AS ticket_promedio_BRL,
        ROUND(AVG(review_score), 2)         AS satisfaccion_promedio,
        ROUND(AVG(delivery_days), 1)        AS dias_entrega_promedio,
        SUM(CASE WHEN delivery_delay_days > 0 THEN 1 ELSE 0 END) AS entregas_tardias
    FROM {GOLD_FULL}.fact_order_items
    WHERE order_status = 'DELIVERED'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Resumen Final Gold Layer

# COMMAND ----------

print("\n" + "="*70)
print("🏁 GOLD LAYER COMPLETADO EXITOSAMENTE")
print("="*70)
print(f"""
Modelo Estrella disponible en: {GOLD_FULL}

📐 TABLAS DIMENSIÓN:
   • dim_date      → Calendario completo con atributos temporales
   • dim_customer  → Clientes con ciudad, estado y región
   • dim_product   → Productos con categoría, peso y volumen
   • dim_seller    → Vendedores con ubicación geográfica
   • dim_payment   → Métodos y cuotas de pago

📊 TABLA DE HECHOS:
   • fact_order_items → Granularidad: ítem por orden
     Métricas: precio, flete, valor total, días entrega, retraso, review score

🔭 VISTAS PRE-COMPUTADAS PARA POWER BI:
   • vw_ventas_por_categoria_mes
   • vw_performance_vendedores
   • vw_analisis_pagos
   • vw_satisfaccion_por_estado

✅ Listo para conectar Power BI al modelo estrella en {GOLD_FULL}
""")
print(f"   Timestamp: {datetime.now().isoformat()}")
