# 🏗️ Pipeline Medallion — Global Super Store

Proyecto de Data Engineering desarrollado en Databricks Free Edition.
Implementa una **arquitectura medallion** completa con la data de ventas online via **GitHub API**, después de  tener las capas modeladas y cargadas, se consume la capa gold en **Power BI** para la generación de reportería.
La orquestación se realiza mediante un **job en Databricks Free Edition** que presenta un registro log de las ejecuciones y un schedule diario a las 14:00.

## Objetivo

Modelar la data de ventas online bajo un modelo estrella, separando ['Paises','Productos','Clientes','Shipping'], que optimice su carga y facilite la generación de KPI's para una reportería.


## 📐 Arquitectura
<img width="851" height="347" alt="Medallion drawio (2)" src="https://github.com/user-attachments/assets/2187ec03-d7a0-4e87-8dae-02573cb11f2e" />

## 🛠️ Stack tecnológico

- **Orquestación:**
    - **Databricks Jobs**: Orquestador utilizado para la programación del pipeline, se utilizan ejecuciones de notebooks, un schedule para automatizar la ejecución y un notify para fallos en ejecuciones.
- **Fuente:**
    - **GitHub API** → **Kaggle Global Super Store Dataset** : Es la fuente que contiene el dataset de kaggle 'Global Super Store' (https://www.kaggle.com/datasets/apoorvaappz/global-super-store-dataset) que provee las ventas online de distintos paises.
- **Lenguaje:**
    - **Python (PySpark)**: Lenguaje principal utilizado para la ingesta y transformación.
    - **Pandas**: Biblioteca para manipulación y generación de df para lectura y carga en tablas.
- **Almacenamiento:**
    - **Delta Lake**: Formato de almacenamiento de datos construido sobre archivos Parquet utilizado en todas las capas del proyecto, exceptuando la RAW .


## ⚙️ Configuración

### Token GitHub
Crear un Fine-grained token con permisos:
- Contents: Read-only
- Metadata: Read-only
Este token es necesario ya que el dataset se encuentra en un repositorio github y para la conexión directa, se solicitará un token, pero los permisos son los minimos necesarios.

- ### Job Parameters
| Parámetro | Valor |
|---|---|
| github_owner | johnek96 |
| github_repo | Proyecto-Python-Data-Engineering |
| github_branch | main |
| github_path | Global_Superstore2.csv |

Con estos parámetros y el token, el job tiene los datos necesarios para ejecutar el primer task 'RAW', desde allí, las demás tareas se ejecutarán en secuencia, excepto en la capa gold que los dim_ se ejecutan en paralelo y el fact_ al final.

<img width="1215" height="350" alt="Job" src="https://github.com/user-attachments/assets/ccec33bf-69b7-4959-817e-668db1c76da1" />

## Logs
Cuando haya concluido la ejecución del job, la última tarea registra la carga y ejecución en un delta table con los registros.

Para consultar los registros de esta tabla, ejecutar en un notebook el siguiente script:

%sql
-- %sql Consultar historial de ejecuciones
SELECT
    execution_ts,
    job_name,
    bronze_rows,
    silver_rows,
    fact_rows,
    status,
    error_message
FROM workspace.logs.pipeline_execution_log
ORDER BY execution_ts DESC;

## 👤 Autor
**johnek96** — Proyecto del curso de Data Engineering


