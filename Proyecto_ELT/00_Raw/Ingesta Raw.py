# Databricks notebook source
import requests, hashlib, os
from datetime import datetime

# ── Variables directas ───────────────────────────────────────
GITHUB_OWNER  = "johnek96"
GITHUB_REPO   = "Proyecto-Python-Data-Engineering"
GITHUB_BRANCH = "main"
GITHUB_PATH   = "Global_Superstore2.csv"
GITHUB_TOKEN  = "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"   # ← tu token real aquí
VOLUME_PATH   = "/Volumes/workspace/default/capa_raw"
FILE_NAME     = "Global_Superstore2.csv"
DEST_PATH     = f"{VOLUME_PATH}/{FILE_NAME}"

# ── Descarga via download_url (archivo > 1MB) ─────────────────
api_url  = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{GITHUB_PATH}"
headers  = {
    "Authorization"       : f"Bearer {GITHUB_TOKEN}",
    "Accept"              : "application/vnd.github+json",
    "X-GitHub-API-Version": "2022-11-28",
}

# Paso 1 — obtener download_url
response     = requests.get(api_url, headers=headers, params={"ref": GITHUB_BRANCH}, timeout=30)
data         = response.json()
download_url = data.get("download_url")
file_size    = data.get("size", 0)

print(f"  Tamaño    : {file_size / 1024 / 1024:.2f} MB")
print(f"  URL raw   : {download_url}")

# Paso 2 — descargar contenido real
dl = requests.get(
    download_url,
    headers={"Authorization": f"Bearer {GITHUB_TOKEN}"},
    timeout=120,
    stream=True,
)
dl.raise_for_status()

chunks = []
total  = 0
for chunk in dl.iter_content(chunk_size=1024 * 1024):
    if chunk:
        chunks.append(chunk)
        total += len(chunk)
        print(f"  Descargado: {total / 1024 / 1024:.1f} MB...", end="\r")

file_bytes = b"".join(chunks)
print(f"\n  ✓ Descarga completa : {len(file_bytes) / 1024 / 1024:.2f} MB")

# Paso 3 — guardar en Volumes
with open(DEST_PATH, "wb") as f:
    f.write(file_bytes)

written = os.path.getsize(DEST_PATH)
print(f"  ✓ Guardado en       : {DEST_PATH}")
print(f"  ✓ Tamaño verificado : {written:,} bytes")
print(f"  ✓ MD5               : {hashlib.md5(file_bytes).hexdigest()}")

# Paso 4 — verificar con Spark
df = spark.read.option("header", "true").csv(DEST_PATH)
print(f"  ✓ Filas             : {df.count():,}")
print(f"  ✓ Columnas          : {len(df.columns)}")
print(f"\n  ✅ Capa RAW lista — archivo en {DEST_PATH}")
