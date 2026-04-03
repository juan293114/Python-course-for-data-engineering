# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Orquestación del Pipeline — Configuración del Databricks Job
# MAGIC ## Pipeline ELT | Arquitectura Medallón | Olist Brazilian E-Commerce
# MAGIC
# MAGIC ### Este notebook cumple dos funciones:
# MAGIC 1. **Documentación técnica** de cómo configurar el Job en Databricks UI
# MAGIC 2. **Script de utilidad** para crear/actualizar el Job vía Databricks REST API
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Topología del Job (DAG)
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                  olist_ecommerce_elt_pipeline                    │
# MAGIC │                  (Cron: diario 02:00 AM UTC-5)                   │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC           │
# MAGIC           ▼
# MAGIC   ┌──────────────────┐
# MAGIC   │  Task 1          │
# MAGIC   │  BRONZE LAYER    │  01_bronze_layer.py
# MAGIC   │  (Ingesta Raw)   │
# MAGIC   └────────┬─────────┘
# MAGIC            │  (depende de: ninguna)
# MAGIC            ▼
# MAGIC   ┌──────────────────┐
# MAGIC   │  Task 2          │
# MAGIC   │  SILVER LAYER    │  02_silver_layer.py
# MAGIC   │  (Limpieza)      │
# MAGIC   └────────┬─────────┘
# MAGIC            │  (depende de: Task 1)
# MAGIC            ▼
# MAGIC   ┌──────────────────┐
# MAGIC   │  Task 3          │
# MAGIC   │  GOLD LAYER      │  03_gold_layer.py
# MAGIC   │  (Modelo ★)      │
# MAGIC   └────────┬─────────┘
# MAGIC            │  (depende de: Task 2)
# MAGIC            ▼
# MAGIC   ┌──────────────────┐
# MAGIC   │  Task 4          │
# MAGIC   │  NOTIFICATION    │  04_job_orchestration.py
# MAGIC   │  (Log + Email)   │
# MAGIC   └──────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros del Job

# COMMAND ----------

import requests
import json
from datetime import datetime

# ────────────────────────────────────────────────────────────────
# CONFIGURACIÓN — editar estos valores según tu entorno Databricks
# ────────────────────────────────────────────────────────────────

DATABRICKS_HOST  = "https://<TU-WORKSPACE>.azuredatabricks.net"  # Ej: https://adb-1234567890.azuredatabricks.net
DATABRICKS_TOKEN = "<TU-PAT-TOKEN>"                               # Personal Access Token de Databricks

# Rutas de los notebooks (relativas al Workspace raíz)
NOTEBOOK_BASE_PATH = "/Repos/<TU-EMAIL>/olist-ecommerce-pipeline/notebooks"

# Cluster config
CLUSTER_CONFIG = {
    "num_workers": 2,
    "spark_version": "14.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",      # Azure / ajustar según cloud
    "autoscale": {
        "min_workers": 1,
        "max_workers": 4
    }
}

# Parámetros globales del pipeline
PIPELINE_PARAMS = {
    "catalog":        "workspace",
    "bronze_schema":  "bronze_ecommerce",
    "silver_schema":  "silver_ecommerce",
    "gold_schema":    "gold_ecommerce",
    "raw_path":       "/Volumes/workspace/raw/olist_data"
}

print("⚙️  Configuración del Job cargada")
print(f"   Host     : {DATABRICKS_HOST}")
print(f"   Notebooks: {NOTEBOOK_BASE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Definición del Job (JSON para la API)

# COMMAND ----------

job_definition = {
    "name": "olist_ecommerce_elt_pipeline",

    # ── PROGRAMACIÓN DEL JOB ──────────────────────────────────────
    # Ejecutar todos los días a las 02:00 AM (hora Colombia UTC-5)
    # En cron, UTC-5 → 07:00 AM UTC
    "schedule": {
        "quartz_cron_expression": "0 0 7 * * ?",   # Diariamente 02:00 AM Colombia
        "timezone_id": "America/Bogota",
        "pause_status": "UNPAUSED"
    },

    # ── CLUSTER COMPARTIDO PARA TODAS LAS TAREAS ─────────────────
    "job_clusters": [
        {
            "job_cluster_key": "olist_cluster",
            "new_cluster": {
                **CLUSTER_CONFIG,
                "spark_conf": {
                    "spark.databricks.delta.preview.enabled": "true",
                    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                },
                "spark_env_vars": {
                    "PIPELINE_ENV": "production"
                }
            }
        }
    ],

    # ── DEFINICIÓN DE TAREAS (DAG SECUENCIAL) ────────────────────
    "tasks": [
        # ─── TASK 1: Bronze Layer ───
        {
            "task_key": "bronze_layer",
            "description": "Ingesta de CSVs desde Raw y carga como Delta Tables en Bronze",
            "notebook_task": {
                "notebook_path": f"{NOTEBOOK_BASE_PATH}/01_bronze_layer",
                "base_parameters": {
                    "raw_path":       PIPELINE_PARAMS["raw_path"],
                    "bronze_schema":  PIPELINE_PARAMS["bronze_schema"],
                    "catalog":        PIPELINE_PARAMS["catalog"]
                },
                "source": "WORKSPACE"
            },
            "job_cluster_key": "olist_cluster",
            "timeout_seconds": 3600,       # 1 hora máximo
            "max_retries": 2,
            "min_retry_interval_millis": 60000,   # Retry después de 1 minuto
            "retry_on_timeout": False,
            "email_notifications": {
                "on_failure": ["<TU-EMAIL>@gmail.com"],
                "on_success": []
            }
        },

        # ─── TASK 2: Silver Layer ───
        {
            "task_key": "silver_layer",
            "description": "Limpieza, estandarización y validación de datos en Silver",
            "depends_on": [{"task_key": "bronze_layer"}],   # ← Dependencia
            "notebook_task": {
                "notebook_path": f"{NOTEBOOK_BASE_PATH}/02_silver_layer",
                "base_parameters": {
                    "bronze_schema": PIPELINE_PARAMS["bronze_schema"],
                    "silver_schema": PIPELINE_PARAMS["silver_schema"],
                    "catalog":       PIPELINE_PARAMS["catalog"]
                },
                "source": "WORKSPACE"
            },
            "job_cluster_key": "olist_cluster",
            "timeout_seconds": 3600,
            "max_retries": 1,
            "min_retry_interval_millis": 60000,
            "retry_on_timeout": False
        },

        # ─── TASK 3: Gold Layer ───
        {
            "task_key": "gold_layer",
            "description": "Construcción del Modelo Estrella en Gold para Power BI",
            "depends_on": [{"task_key": "silver_layer"}],   # ← Dependencia
            "notebook_task": {
                "notebook_path": f"{NOTEBOOK_BASE_PATH}/03_gold_layer",
                "base_parameters": {
                    "silver_schema": PIPELINE_PARAMS["silver_schema"],
                    "gold_schema":   PIPELINE_PARAMS["gold_schema"],
                    "catalog":       PIPELINE_PARAMS["catalog"]
                },
                "source": "WORKSPACE"
            },
            "job_cluster_key": "olist_cluster",
            "timeout_seconds": 3600,
            "max_retries": 1,
            "min_retry_interval_millis": 60000,
            "retry_on_timeout": False
        },

        # ─── TASK 4: Notificación y Log Final ───
        {
            "task_key": "pipeline_notification",
            "description": "Envío de notificación y registro de ejecución exitosa",
            "depends_on": [{"task_key": "gold_layer"}],     # ← Dependencia
            "notebook_task": {
                "notebook_path": f"{NOTEBOOK_BASE_PATH}/04_job_orchestration",
                "base_parameters": {
                    "task_mode": "notification",
                    "catalog":   PIPELINE_PARAMS["catalog"]
                },
                "source": "WORKSPACE"
            },
            "job_cluster_key": "olist_cluster",
            "timeout_seconds": 600
        }
    ],

    # ── NOTIFICACIONES GLOBALES ───────────────────────────────────
    "email_notifications": {
        "on_start":   [],
        "on_success": ["<TU-EMAIL>@gmail.com"],
        "on_failure": ["<TU-EMAIL>@gmail.com"],
        "no_alert_for_skipped_runs": True
    },

    # ── CONFIGURACIÓN DEL JOB ─────────────────────────────────────
    "max_concurrent_runs": 1,
    "format": "MULTI_TASK",

    # Retención de historial de ejecuciones (90 días)
    "run_as": {
        "user_name": "<TU-EMAIL>@gmail.com"
    }
}

print("✅ Definición del Job construida")
print(f"   Nombre del Job: {job_definition['name']}")
print(f"   Número de tareas: {len(job_definition['tasks'])}")
print(f"   Schedule: {job_definition['schedule']['quartz_cron_expression']} ({job_definition['schedule']['timezone_id']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Crear o Actualizar el Job via API

# COMMAND ----------

def get_headers():
    return {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

def create_or_update_job(job_def: dict) -> dict:
    """
    Crea el Job si no existe. Si existe (por nombre), lo actualiza (reset).
    Retorna el job_id y la acción realizada.
    """
    headers = get_headers()

    # 1. Buscar si el job ya existe
    list_url = f"{DATABRICKS_HOST}/api/2.1/jobs/list"
    resp = requests.get(list_url, headers=headers, params={"name": job_def["name"]})
    resp.raise_for_status()

    jobs = resp.json().get("jobs", [])
    existing_job = next((j for j in jobs if j["settings"]["name"] == job_def["name"]), None)

    if existing_job:
        # Actualizar job existente
        job_id = existing_job["job_id"]
        reset_url = f"{DATABRICKS_HOST}/api/2.1/jobs/reset"
        payload = {"job_id": job_id, "new_settings": job_def}
        resp = requests.post(reset_url, headers=headers, json=payload)
        resp.raise_for_status()
        print(f"🔄 Job actualizado — ID: {job_id}")
        return {"job_id": job_id, "action": "UPDATED"}
    else:
        # Crear nuevo job
        create_url = f"{DATABRICKS_HOST}/api/2.1/jobs/create"
        resp = requests.post(create_url, headers=headers, json=job_def)
        resp.raise_for_status()
        job_id = resp.json()["job_id"]
        print(f"✅ Job creado — ID: {job_id}")
        return {"job_id": job_id, "action": "CREATED"}


# ── SOLO EJECUTAR si el token está configurado ──
if DATABRICKS_TOKEN != "<TU-PAT-TOKEN>":
    result = create_or_update_job(job_definition)
    print(f"\n🎯 Job ID: {result['job_id']}")
    print(f"   Acción : {result['action']}")
    print(f"   URL    : {DATABRICKS_HOST}/#job/{result['job_id']}")
else:
    print("⚠️  Token no configurado. Edita DATABRICKS_TOKEN para ejecutar la API.")
    print("   Alternativamente, usa la UI de Databricks con el JSON de job_definition.")
    print(f"\n📋 JSON del Job para pegarlo en la UI:\n")
    print(json.dumps(job_definition, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Modo Notificación (ejecutado por el pipeline como Task 4)

# COMMAND ----------

# Solo se ejecuta cuando task_mode == "notification"
task_mode = dbutils.widgets.get("task_mode") if "task_mode" in [w.name for w in dbutils.widgets.getAll()] else "standalone"

if task_mode == "notification":
    from pyspark.sql.functions import current_timestamp, lit

    print("📧 Enviando notificación de finalización del pipeline...")

    # Registrar log de ejecución exitosa en Delta Table
    CATALOG = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.getAll()] else "main"

    log_entry = spark.createDataFrame([{
        "pipeline_name":      "olist_ecommerce_elt_pipeline",
        "execution_datetime": datetime.now().isoformat(),
        "status":             "SUCCESS",
        "bronze_layer":       "COMPLETED",
        "silver_layer":       "COMPLETED",
        "gold_layer":         "COMPLETED",
        "notes":              "Pipeline ELT Medallion completado exitosamente."
    }])

    (log_entry.write
     .format("delta")
     .mode("append")
     .saveAsTable(f"workspace.bronze_ecommerce.pipeline_execution_log"))

    print("✅ Log de ejecución registrado en pipeline_execution_log")
    print(f"   Timestamp: {datetime.now().isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Instrucciones para Configurar el Job Manualmente en la UI

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════╗
║       INSTRUCCIONES — Configurar Job en Databricks UI            ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  1. Ir a: Databricks Workspace → Workflows → Create Job          ║
║                                                                  ║
║  2. Nombre del Job: olist_ecommerce_elt_pipeline                 ║
║                                                                  ║
║  3. Agregar Tasks en orden:                                       ║
║     Task 1: bronze_layer → 01_bronze_layer (sin dependencia)     ║
║     Task 2: silver_layer → 02_silver_layer (depende de Task 1)   ║
║     Task 3: gold_layer   → 03_gold_layer   (depende de Task 2)   ║
║     Task 4: notification → 04_job_orche... (depende de Task 3)   ║
║                                                                  ║
║  4. Para cada Task: seleccionar "Job Compute" y crear cluster     ║
║     con las especificaciones definidas en CLUSTER_CONFIG         ║
║                                                                  ║
║  5. Schedule:                                                     ║
║     • Tipo: Cron                                                  ║
║     • Expresión: 0 0 7 * * ?  (02:00 AM Colombia)               ║
║     • Timezone: America/Bogota                                    ║
║                                                                  ║
║  6. Email Notifications:                                          ║
║     • On Start: (vacío)                                           ║
║     • On Success: tu correo                                       ║
║     • On Failure: tu correo                                       ║
║                                                                  ║
║  7. Conectar Repo GitHub:                                         ║
║     Repos → Add Repo → URL del GitHub → Token del archivo .txt   ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
""")
