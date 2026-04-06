from pyspark.sql.functions import col, round, to_date, lag
from pyspark.sql.window import Window

# Leer desde Bronze (usando el catálogo y esquema creado)
df_bronze = spark.table("j_grupo_10.bronze.stock_data")

# Definir una ventana para cálculos de series temporales
windowSpec = Window.partitionBy("ticker").orderBy("Date")   # (retorno diario)

# Transformaciones: Limpieza, casteo de tipos y métricas financieras
df_silver = df_bronze.dropDuplicates() \
    .dropna(subset=["Close", "Volume"]) \
    .withColumn("Date", to_date(col("Date"))) \
    .withColumn("Close", round(col("Close"), 2)) \
    .withColumn("Volume", col("Volume").cast("long")) \
    .withColumn("Daily_Return", round((col("Close") - lag("Close", 1).over(windowSpec)) / lag("Close", 1).over(windowSpec), 4))

# Guardar como tabla Delta en Silver
spark.sql("DROP TABLE IF EXISTS j_grupo_10.silver.stock_data")
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("j_grupo_10.silver.stock_data")

print("Capa SILVER completada: Datos limpios y enriquecidos.")