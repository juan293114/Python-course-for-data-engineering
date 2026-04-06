%pip install yfinance
%restart_python

import os
import pandas as pd
import yfinance as yf
from pyspark.sql.window import Window
from pyspark.sql.functions import col, round, to_date, lag

# Configuración en Databricks
spark.sql("CREATE CATALOG IF NOT EXISTS j_grupo_10")
spark.sql("CREATE SCHEMA IF NOT EXISTS j_grupo_10.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS j_grupo_10.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS j_grupo_10.gold")
spark.sql("CREATE VOLUME IF NOT EXISTS j_grupo_10.bronze.raw_volume")

# Extracción (De yfinance a RAW)
tickers = ["AAPL", "MSFT", "TSLA", "SPY"]

data = []

for ticker in tickers:
    df = yf.download(ticker, start="2024-01-01", end="2025-12-31")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)
    df["ticker"] = ticker
    df.reset_index(inplace=True)
    data.append(df)

df_final = pd.concat(data)

# Guardar en Volume
ruta_raw_csv = "/Volumes/j_grupo_10/bronze/raw_volume/stock_data.csv"
os.makedirs(os.path.dirname(ruta_raw_csv), exist_ok=True)
df_final.to_csv(ruta_raw_csv, index=False)