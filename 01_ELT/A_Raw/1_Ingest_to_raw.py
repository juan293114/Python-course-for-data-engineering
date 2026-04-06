# Databricks notebook source
# DBTITLE 1,Instalación de paquets
# MAGIC %pip install pendulum

# COMMAND ----------

# DBTITLE 1,Importación de librerías
from pathlib import Path
import json
import time
#import pendulum  #manejar fechas
import requests

# COMMAND ----------

# DBTITLE 1,Declaración de rutas
# Detecta el catálogo actual (suele ser 'workspace' o 'main')
current_catalog = spark.sql("select current_catalog()").first()[0]
catalog = current_catalog
schema = "raw"
volume = "tvmze"

# COMMAND ----------

# DBTITLE 1,Creación de esquema y volumen

# Crea esquema y volumen si no existen
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

# Ruta base
base_path = f"/Volumes/{catalog}/{schema}/{volume}"

# COMMAND ----------

# DBTITLE 1,Eliminar archivos de raw

# Elimina todos los archivos y subcarpetas dentro de la ruta
#dbutils.fs.rm("/Volumes/workspace/raw/tvmze", recurse=True)
dbutils.fs.rm(base_path, recurse=True)


# COMMAND ----------

# DBTITLE 1,Creación de widgets
#dbutils.widgets.text("start_date", "2023-01-01", "Start Date (YYYY-MM-DD)")
#dbutils.widgets.text("end_date", "2023-12-31", "End Date (YYYY-MM-DD)")
#dbutils.widgets.text("output_dir", "/Volumes/workspace/raw/tvmze", "Output Directory")
#dbutils.widgets.text("api_enpoint", "https://api.tvmaze.com/schedule/web", "API endpoint")
#dbutils.widgets.text("timeout", "30", "Time Out")

dbutils.widgets.text("output_dir", base_path, "Output Directory")
dbutils.widgets.text("api_key", "", "API Key")
dbutils.widgets.text("access_token", "", "Access Token")
dbutils.widgets.text("total_pages", "5", "Total pages to fetch")
dbutils.widgets.text("timeout", "30", "Timeout")


# COMMAND ----------

# DBTITLE 1,Consultar widgets
# Leer valores de los widgets
#start_date = pendulum.parse(dbutils.widgets.get("start_date")).date()
#end_date = pendulum.parse(dbutils.widgets.get("end_date")).date()
#output_dir = Path(dbutils.widgets.get("output_dir"))
#endpoint_dir = dbutils.widgets.get("api_enpoint")
#API_URL = endpoint_dir
#timeout = int(dbutils.widgets.get("timeout"))


# Leer valores de los widgets
output_dir = dbutils.widgets.get("output_dir")
API_KEY = dbutils.widgets.get("api_key")
ACCESS_TOKEN = dbutils.widgets.get("access_token")

TOTAL_PAGES = int(dbutils.widgets.get("total_pages"))
timeout = int(dbutils.widgets.get("timeout"))



# COMMAND ----------

# DBTITLE 1,Ingesta api key
saved_files = []

session = requests.Session()

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}


for page in range(1, TOTAL_PAGES + 1):

    try:
        #Obtener listado de películas
        list_response = session.get(
            "https://api.themoviedb.org/3/movie/popular",
            headers=headers,
            params={
                "api_key": API_KEY,
                "page": page
            },
            timeout=timeout
        )

        list_response.raise_for_status()
        data = list_response.json()

    except requests.RequestException:
        continue

    movies = data.get("results", [])

    for movie in movies:

        movie_id = movie.get("id")

        if not movie_id:
            continue

        try:
            #Guardar detalle
            detail_url = f"https://api.themoviedb.org/3/movie/{movie_id}"

            detail_res = session.get(
                detail_url,
                headers=headers,
                params={"api_key": API_KEY},
                timeout=timeout
            )

            detail_res.raise_for_status()

            dbutils.fs.put(
                f"{output_dir}/movies/{movie_id}.json",
                detail_res.text,
                overwrite=True
            )

            saved_files.append(f"movies/{movie_id}")

            #Guardar créditos (actores)
            credits_url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits"

            credits_res = session.get(
                credits_url,
                headers=headers,
                params={"api_key": API_KEY},
                timeout=timeout
            )

            credits_res.raise_for_status()

            dbutils.fs.put(
                f"{output_dir}/credits/{movie_id}.json",
                credits_res.text,
                overwrite=True
            )

            saved_files.append(f"credits/{movie_id}")

            #Control de rate limit
            time.sleep(0.25)

        except requests.RequestException:
            continue

print(f"Archivos guardados: {len(saved_files)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ----- FIN ----

# COMMAND ----------

#IDENTIFICAR ESTRUCTURA JSON MOVIES
df = spark.read.json("/Volumes/workspace/raw/tvmze/movies/1010581.json")
df.printSchema()

# COMMAND ----------

#IDENTIFICAR ESTRUCTURA JSON CREDITOS
df = spark.read.json("/Volumes/workspace/raw/tvmze/credits/1010581.json")
df.printSchema()

# COMMAND ----------

#Estructura
#{
#  "page": 1,
#  "results": [ {...}, {...}, {...} ],
#  "total_pages": 55704,
#  "total_results": 1114062
#}
#from pyspark.sql.functions import explode
#df_movies = df.select(explode("results").alias("movie")) explanar 

df_movies = spark.read.json("/Volumes/workspace/raw/tvmze/movies/1010581.json") #PARA 1 ARCHIVO/DIA
display(df_movies)

# COMMAND ----------

df_credits = spark.read.json("/Volumes/workspace/raw/tvmze/credits/1010581.json") #PARA 1 ARCHIVO/DIA
display(df_credits)
