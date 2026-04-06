# Definir Ruta de RAW
ruta_raw_csv = "/Volumes/j_grupo_10/bronze/raw_volume/stock_data.csv"

# Obtener el DataFrame de RAW
df_raw = spark.read.csv(ruta_raw_csv, header=True, inferSchema=True)

# Guardar como tabla Delta (Bronze)
spark.sql("DROP TABLE IF EXISTS j_grupo_10.bronze.stock_data")
df_raw.write.format("delta").mode("overwrite").saveAsTable("j_grupo_10.bronze.stock_data")
print("Capa BRONZE completada: Tabla Delta creada exitosamente.")