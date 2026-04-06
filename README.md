# Proyecto Final: Pipeline ELT Financiero con Arquitectura Medallón
---
**Grupo:** J10  
**Contexto del Proyecto**   
Simulación de un proceso ELT moderno para el análisis de riesgo de mercado y liquidez de activos financieros, utilizando la arquitectura medallón y orquestación integral.

## Objetivo
Diseñar e implementar un pipeline de datos automatizado que extraiga, transforme y cargue datos históricos del mercado de valores, estructurándolos para su posterior visualización en herramientas de Business Intelligence y respondiendo a preguntas críticas de negocio.

## Stack Tecnológico
* **Lenguaje:** Python, PySpark.
* **Plataforma de Datos:** Databricks (Community Edition).
* **Almacenamiento:** Delta Lake, DBFS.
* **Visualización:** Power BI.
* **Fuente de Datos:** API Pública de Yahoo Finance (`yfinance`).
---

## Arquitectura del Pipeline

El flujo de datos sigue estrictamente la Arquitectura Medallón, garantizando calidad, trazabilidad y gobierno de datos en cada etapa:

### 1. Extracción y Capa Raw 
* **Fuente:** Datos de mercado (Tickers: AAPL, MSFT, TSLA, SPY) extraídos vía API (`yfinance`).
* **Proceso:** Descarga de series temporales de precios y volúmenes. Los datos se almacenan en su formato nativo (`.csv`) sin transformaciones para preservar la integridad del origen.

### 2. Capa Bronze (Ingesta Cruda)
* **Proceso:** Lectura del archivo plano de la capa Raw y conversión estandarizada al formato columnar Delta (`Delta Tables`).
* **Objetivo:** Homogeneizar el formato de almacenamiento para optimizar las lecturas con Spark.

### 3. Capa Silver (Transformación y Limpieza)
* **Proceso:** Limpieza de valores nulos, eliminación de duplicados y casteo de tipos de datos consistentes.
* **Enriquecimiento:** Creación de reglas de negocio, incluyendo el cálculo analítico del **Retorno Diario (Daily Return)** utilizando funciones de ventana (Window Functions) para evaluar la volatilidad.

### 4. Capa Gold (Modelado Dimensional)
* **Proceso:** Creación de un Modelo Estrella optimizado para BI.
* **Estructura:**
  * `Dim_Date`: Dimensión de tiempo con jerarquías (Año, Mes, Día de la semana).
  * `Dim_Ticker`: Dimensión con los identificadores únicos de los activos.
  * `Fact_Market_Data`: Tabla de hechos centralizada con las métricas clave (Apertura, Cierre, Volumen, Retornos).
---

## Orquestación (Databricks Jobs)

El pipeline completo es gestionado por un **Notebook Orquestador**. Mediante el uso de comandos `dbutils`, se ejecutan secuencialmente las ingestas y transformaciones (Raw Bronze => Silver => Gold) asegurando la dependencia lógica de las capas. El flujo está diseñado para ser calendarizado (Scheduled) asegurando actualizaciones periódicas.

---

# Visualización de Datos y Casos de Negocio
El producto final se materializa en un dashboard interactivo construido en Power BI, directamente alimentado por el modelo estrella de la Capa Gold.

**Preguntas de negocio resueltas:**
1. *Riesgo y Volatilidad:* ¿Cuál ha sido la evolución de los retornos diarios promedio y el comportamiento de cierre de los activos tecnológicos frente al índice SPY?
2. *Análisis de Liquidez:* ¿Qué activo y en qué periodo presentó el mayor volumen de transacciones en el mercado?
---

## Instrucciones de Ejecución
1. Clonar el repositorio y acceder a la rama `feature/J_grupo_10`.
2. Importar los notebooks a un entorno de Databricks.
3. Ejecutar el script `Orquestador_Job` (o lanzar el Job programado en la interfaz de Workflows).
4. Abrir el archivo `.pbix` en Power BI Desktop para interactuar con el modelo de datos actualizado.