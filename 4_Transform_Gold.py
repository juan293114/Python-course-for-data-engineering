from pyspark.sql.functions import col, year, month, dayofweek, monotonically_increasing_id

# Leer desde Silver 
df_silver = spark.table("j_grupo_10.silver.stock_data")

# DIMENSIÓN 1: Dim_Ticker
# Extracción de tickers únicos y asignación de ID numérico
dim_ticker = df_silver.select("ticker").distinct().withColumn("ticker_id", monotonically_increasing_id())

spark.sql("DROP TABLE IF EXISTS j_grupo_10.gold.dim_ticker")
dim_ticker.write.format("delta").mode("overwrite").saveAsTable("j_grupo_10.gold.dim_ticker")

# DIMENSIÓN 2: Dim_Date
# Extracción de fechas únicas y generación de jerarquías de tiempo
dim_date = df_silver.select("Date").distinct() \
    .withColumn("Year", year(col("Date"))) \
    .withColumn("Month", month(col("Date"))) \
    .withColumn("DayOfWeek", dayofweek(col("Date")))

spark.sql("DROP TABLE IF EXISTS j_grupo_10.gold.dim_date")
dim_date.write.format("delta").mode("overwrite").saveAsTable("j_grupo_10.gold.dim_date")

# TABLA DE HECHOS: Fact_Market_Data
# Unión con dim_ticker para reemplazar el string del ticker por su ID relacional
fact_market = df_silver.join(dim_ticker, "ticker", "left") \
    .select(col("Date").alias("date_id"), "ticker_id", "Open", "Close", "Volume", "Daily_Return")

spark.sql("DROP TABLE IF EXISTS j_grupo_10.gold.fact_market_data")
fact_market.write.format("delta").mode("overwrite").saveAsTable("j_grupo_10.gold.fact_market_data")

print("Capa GOLD completada: Modelo Estrella generado exitosamente.")