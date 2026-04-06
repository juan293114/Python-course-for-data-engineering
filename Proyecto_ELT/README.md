# 🏗️ Pipeline Medallion — Global Super Store

Proyecto de Data Engineering desarrollado en Databricks Free Edition.
Implementa una arquitectura medallion completa con la data de ventas online via GitHub API, después de  tener las capas modeladas y cargadas, se consume la capa gold en Power BI para la generación de reportería.
La orquestación se realiza mediante un job en Databricks Free Edition que presenta un registro log de las ejecuciones y un schedule diario a las 14:00.

## Objetivo

Modelar la data de ventas online bajo un modelo estrella, separando ['Paises','Productos','Clientes','Shipping'], que optimice su carga y facilite la generación de KPI's para una reportería.


## 📐 Arquitectura
<img width="851" height="347" alt="Medallion drawio (2)" src="https://github.com/user-attachments/assets/2187ec03-d7a0-4e87-8dae-02573cb11f2e" />

## 🛠️ Stack tecnológico

- **Plataforma:** Databricks Free Edition
- **Lenguaje:** Python (PySpark + pandas)
- **Formato:** Delta Lake
- **Orquestación:** Databricks Jobs
- **Fuente:** GitHub API → Kaggle Global Super Store Dataset

