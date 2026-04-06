# Databricks notebook source
import pandas as pd
from datetime import datetime

# COMMAND ----------

t_start = datetime.now()

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.gold.fact_orders")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gold.fact_orders (
  Row_ID INT,
  Order_ID STRING,
  Order_Date DATE,
  Ship_Date DATE,
  Customer_sk BIGINT,
  Product_sk BIGINT,
  Geography_sk BIGINT,
  Ship_sk BIGINT,
  Sales DOUBLE,
  Quantity INT,
  Discount DOUBLE,
  Profit DOUBLE,
  Shipping_Cost DOUBLE,
  Order_Priority STRING,
  profit_margin_pct DOUBLE,
  revenue_after_discount DOUBLE,
  flg_profitable INT,
  _ingestion_ts TIMESTAMP
)
""")

# COMMAND ----------

df = spark.read.table("workspace.silver.Super_Store").toPandas()

# COMMAND ----------

dim_customer  = spark.read.table("workspace.gold.dim_customer").toPandas()
dim_product   = spark.read.table("workspace.gold.dim_product").toPandas()
dim_geography = spark.read.table("workspace.gold.dim_geography").toPandas()
dim_shipping  = spark.read.table("workspace.gold.dim_shipping").toPandas()

print(f"  ✓ Dimensiones cargadas:")
print(f"    dim_customer  : {len(dim_customer):,}")
print(f"    dim_product   : {len(dim_product):,}")
print(f"    dim_geography : {len(dim_geography):,}")
print(f"    dim_shipping  : {len(dim_shipping):,}")

# COMMAND ----------

# Resolver FKs con merge
fact = df.copy()
 
# FK customer
fact = fact.merge(
    dim_customer[["Customer_sk", "Customer_ID"]],
    on="Customer_ID",
    how="left"
)
 
# FK product
fact = fact.merge(
    dim_product[["Product_sk", "Product_ID"]],
    on="Product_ID",
    how="left"
)
 
# FK geography
fact = fact.merge(
    dim_geography[["Geography_sk", "City", "State", "Country"]],
    on=["City", "State", "Country"],
    how="left"
)
 
# FK shipping
fact = fact.merge(
    dim_shipping[["Ship_sk", "Ship_Mode"]],
    on=["Ship_Mode"],
    how="left"
)

# COMMAND ----------

# Seleccionar columnas finales
fact_cols = [
    "Row_ID", "Order_ID", "Order_Date", "Ship_Date",
    "Customer_sk", "Product_sk", "Geography_sk", "Ship_sk",
    "Sales", "Quantity", "Discount", "Profit", "Shipping_Cost","Order_Priority",
    "profit_margin_pct","revenue_after_discount", "flg_profitable"
]

# COMMAND ----------

fact = (
    fact
    .reindex(columns=fact_cols)
    .reset_index(drop=True)
)

# COMMAND ----------

# Timestamp de ingesta
fact["_ingestion_ts"] = pd.Timestamp.now()
 
print(f"\n  ✓ fact_order_items construida: {len(fact):,} filas")

# COMMAND ----------

df_spark = spark.createDataFrame(fact)
 
(df_spark
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.fact_orders")
)
 
t_elapsed = (datetime.now() - t_start).total_seconds()
print(f"  ✓ fact_orders escrita: {len(fact):,} filas")
print(f"  ✓ Tiempo total            : {t_elapsed:.2f}s")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="fact_rows", value=int(len(fact)))
