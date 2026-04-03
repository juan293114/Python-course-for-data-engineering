# Proyecto D_GRUPO_4
# Pipeline ELT con Arquitectura Medallón: Bioactividad de Medicamentos (PubChem)

## Integrantes

| Nombre | Rol |
|--------|-----|
| Jeshua Romero Guadarrama | Desarrollo del pipeline y documentación |
| Steven Bernal Castro | Desarrollo del pipeline y documentación |

---

## 1. Introducción

Este proyecto implementa un pipeline **ELT** (Extract, Load, Transform) con **arquitectura medallón** para analizar datos de **bioactividad de medicamentos** extraídos de la API pública de **PubChem** (NIH - Instituto Nacional de Salud de EE.UU.).

El pipeline extrae datos de propiedades químicas y resultados de bioensayos de 15 medicamentos conocidos, los procesa a través de cuatro capas (Raw → Bronce → Plata → Oro), construye un **modelo estrella** dimensional y conecta los resultados a **Power BI** para visualización.

---

## 2. Objetivo del Proyecto

1. **Extraer** datos de bioactividad de medicamentos desde la API de PubChem (directamente desde Databricks)
2. **Cargar** los datos crudos en formato JSON (capa Raw)
3. **Transformar** progresivamente: homogenización (Bronce), limpieza y enriquecimiento (Plata), modelo dimensional (Oro)
4. **Orquestar** con Databricks Jobs de forma calendarizada
5. **Visualizar** indicadores farmacológicos en Power BI

---

## 3. Fuente de Datos

### 3.1 ¿Qué es PubChem?

PubChem es la base de datos pública de química más grande del mundo, mantenida por el NIH. Contiene información sobre millones de compuestos químicos, sus propiedades fisicoquímicas y sus actividades biológicas.

### 3.2 API utilizada: PUG REST

| Característica | Detalle |
|---------------|---------|
| **Nombre** | PubChem PUG REST API |
| **Endpoint** | `https://pubchem.ncbi.nlm.nih.gov/rest/pug` |
| **Autenticación** | No requiere |
| **Datos extraídos** | Propiedades de 15 medicamentos + ~58,000 resultados de bioensayos |
| **Formato** | JSON |

### 3.3 Datos extraídos

**Propiedades de compuestos** (15 medicamentos):
- Fórmula molecular, peso molecular, XLogP (lipofilicidad)
- TPSA (área de superficie polar), complejidad molecular
- Conteos de enlaces de hidrógeno (donantes y aceptores)

**Resultados de bioensayos** (~58,000 registros):
- ID del ensayo (AID), ID del compuesto (CID)
- Resultado: Activo, Inactivo, Inconcluso
- Valor de actividad (en micromoles)
- Tipo de ensayo, gen objetivo, nombre del ensayo

### 3.4 Medicamentos seleccionados

| Medicamento | CID | Categoría terapéutica |
|------------|-----|----------------------|
| Aspirina | 2244 | Analgésico / Antiinflamatorio |
| Ibuprofeno | 3672 | Antiinflamatorio |
| Celecoxib | 2662 | Antiinflamatorio selectivo |
| Paracetamol | 5770 | Analgésico / Antipirético |
| Naproxeno | 5329 | Antiinflamatorio |
| Diazepam | 2719 | Ansiolítico |
| Morfina | 5743 | Analgésico opioide |
| Cafeína | 2519 | Estimulante del SNC |
| Fluoxetina | 3345 | Antidepresivo (ISRS) |
| Metformina | 4091 | Antidiabético |
| Amoxicilina | 2082 | Antibiótico |
| Omeprazol | 5494 | Inhibidor bomba de protones |
| Melatonina | 5311 | Regulador del sueño |
| Nicotina | 4386 | Estimulante |
| Loratadina | 5640 | Antihistamínico |

---

## 4. Arquitectura Medallón

```
API PubChem ──► RAW ──► BRONCE ──► PLATA ──► ORO ──► Power BI
  (NIH)       (JSON)   (Delta)   (Delta)   (Delta)   (reporte)
```

### 4.1 Capa Raw
- Archivos JSON crudos en un Volumen de Unity Catalog
- Dos archivos: `propiedades_*.json` y `bioactividad_*.json`

### 4.2 Capa Bronce
- Dos Delta Tables sin transformaciones de negocio
- `pubchem_bronce.propiedades_compuestos` (15 registros)
- `pubchem_bronce.bioactividad` (~58,000 registros)

### 4.3 Capa Plata
Transformaciones aplicadas:
1. Renombrado de columnas al español
2. Filtrado de registros sin resultado de actividad
3. Casteo de tipos (valor_actividad a double)
4. Eliminación de duplicados
5. Enriquecimiento con propiedades del compuesto (JOIN)
6. Clasificación según Regla de Lipinski (druglikeness)
- Tabla: `pubchem_plata.actividades_biologicas`

### 4.4 Capa Oro - Modelo Estrella

```
                    ┌─────────────────────┐
                    │   dim_compuestos     │
                    │─────────────────────│
                    │llave_compuesto (PK)  │
                    │nombre_comun          │
                    │formula_molecular     │
                    │peso_molecular        │
                    │xlogp                 │
                    │clasificacion_lipinski│
                    └──────────┬──────────┘
                               │
┌─────────────────┐ ┌──────────┴────────────────┐ ┌─────────────────┐
│ dim_resultados  │ │  fact_bioactividades       │ │  dim_ensayos    │
│─────────────────│ │───────────────────────────│ │─────────────────│
│llave_result(PK) │─│llave_bioactividad (PK)    │─│llave_ensayo(PK) │
│resultado_activ  │ │llave_compuesto (FK)       │ │id_ensayo        │
└─────────────────┘ │llave_ensayo (FK)          │ │nombre_ensayo    │
                    │llave_resultado (FK)       │ │tipo_ensayo      │
                    │valor_actividad_um         │ └─────────────────┘
                    │id_gen_objetivo            │
                    │acceso_objetivo            │
                    │id_pubmed                  │
                    └───────────────────────────┘
```

---

## 5. Notebooks del Pipeline

| # | Notebook | Capa | Descripción |
|---|----------|------|-------------|
| 0 | `00_configuracion.ipynb` | Setup | Crea esquemas y volumen |
| 1 | `01_ingesta_raw.ipynb` | Raw | Consume API PubChem → JSON en volumen |
| 2 | `02_raw_a_bronce.ipynb` | Bronce | JSON → Delta Tables |
| 3 | `03_exploracion_datos.ipynb` | Perfilamiento | Análisis descriptivo de los datos |
| 4 | `04_validacion_calidad.ipynb` | Calidad | Data Quality Gate (6 reglas) |
| 5 | `05_bronce_a_plata.ipynb` | Plata | Limpieza, enriquecimiento, Lipinski |
| 6 | `06_plata_a_oro.ipynb` | Oro | Modelo estrella (1 fact + 3 dims) |
| 7 | `07_orquestacion.ipynb` | Pipeline | Ejecuta todo en secuencia |
| 8 | `08_logs_auditoria.ipynb` | Auditoría | Registro de métricas por ejecución |
| 9 | `09_consultas_analiticas.ipynb` | Análisis | 9 consultas farmacológicas |
| 10 | `10_resumen_ejecutivo.ipynb` | Resumen | KPIs y panel de control |

---

## 6. Orquestación con Databricks Jobs

### Tareas del Job (en orden):
```
00_configuracion → 01_ingesta_raw → 02_raw_a_bronce → 03_exploracion_datos
→ 04_validacion_calidad → 05_bronce_a_plata → 06_plata_a_oro → 08_logs_auditoria
```

Calendarización: Diaria a las 06:00 AM UTC.

---

## 7. Reporte en Power BI

Conectado a las tablas del esquema `pubchem_oro`:

1. **¿Qué medicamentos tienen mayor tasa de actividad biológica?**
2. **¿Cuáles cumplen la Regla de Lipinski (drug-likeness)?**
3. **¿Qué tipos de ensayos son más comunes?**
4. **¿Cuáles compuestos son más potentes (menor valor de actividad)?**

---

## 8. Tecnologías

| Tecnología | Uso |
|-----------|-----|
| Databricks (Serverless) | Plataforma de ejecución |
| Apache Spark (PySpark) | Motor de procesamiento |
| Delta Lake | Almacenamiento con transacciones ACID |
| Unity Catalog | Gobernanza y catálogo de datos |
| PubChem API (NIH) | Fuente de datos |
| Python | Lenguaje del pipeline |
| Power BI | Visualización |
| GitHub | Control de versiones |

---

## 9. Estructura del Proyecto

```
README.md
Proyecto_D_GRUPO_4/
└── notebooks/
    ├── 00_configuracion.ipynb
    ├── 01_ingesta_raw.ipynb
    ├── 02_raw_a_bronce.ipynb
    ├── 03_exploracion_datos.ipynb
    ├── 04_validacion_calidad.ipynb
    ├── 05_bronce_a_plata.ipynb
    ├── 06_plata_a_oro.ipynb
    ├── 07_orquestacion.ipynb
    ├── 08_logs_auditoria.ipynb
    ├── 09_consultas_analiticas.ipynb
    └── 10_resumen_ejecutivo.ipynb
```
