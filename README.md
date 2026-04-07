# Python-course-for-data-engineering
## 📋 Descripción

Pipeline de datos siguiendo la arquitectura medallion (Bronze → Silver → Gold) construido en **Databricks** utilizando **Polars**, **Delta Tables** y **Unity Catalog**.

Este proyecto implementa un ETL completo para [tu caso de uso - ej: análisis de e-commerce Olist] con transformaciones incrementales y un data warehouse dimensional para BI.

---

## 🏗️ Estructura del Proyecto

ProyectoFinal/
├── A_Raw/              # Datos crudos (ingestión inicial)
├── B_Bronze/           # Bronze: validación y limpieza básica
├── C_Silver/           # Silver: transformaciones y enriquecimiento
├── D_Gold/             # Gold: tablas fact/dimension para BI
├── lib/                # Funciones reutilizables y utilidades
├── README.md           # Este archivo
└── notebooks/          # Notebooks de exploración y ejecución

---

## 🔄 Capas de la Arquitectura

### **A_Raw** 
- Datos originales sin procesar
- Fuente: Kaggle Olist Dataset
- Formato: Parquet/CSV

### **B_Bronze**
- Validación de esquemas
- Limpieza inicial (valores nulos, tipos de dato)
- Detección de duplicados
- Tablas Delta con historial completo

### **C_Silver**
- Transformaciones de negocio
- Enriquecimiento de datos
- Creación de dimensiones
- Resolución de calidad de datos

### **D_Gold**
- Star Schema para BI
- Tablas Fact y Dimension optimizadas
- Agregaciones pre-calculadas

---

## 🛠️ Tecnologías

- **Databricks**: Plataforma de ejecución
- **Polars**: Transformación de datos (rendimiento)
- **Unity Catalog**: Gobernanza y control de acceso
- **Power BI**: Visualización (conectado a Gold)

---

**Tablas Fact:**
- `fact_orders` - Pedidos y transacciones
- `fact_order_items` - Detalle de ítems

**Tablas Dimension:**
- `dim_customers` - Clientes
- `dim_products` - Productos
- `dim_sellers` - Vendedores
- `dim_date` - Calendario

---

