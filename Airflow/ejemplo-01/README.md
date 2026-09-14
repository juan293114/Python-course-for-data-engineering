# Ejercicio — Clase 4: Executors y Concurrencia
**Curso:** PEDE/9 — Apache Airflow  
**Profesor:** Renato Arrascue  
**Alumno:** Jorge Andres Sanchez  

**Ejercicio 01:** 
Una empresa de delivery tiene un DAG con 6 tasks independientes entre sí (sin dependencias unas de
otras), cada una simulando la generación de un reporte de una ciudad distinta (Lima, Arequipa, Trujillo,
Chiclayo, Piura, Cusco), y cada una tarda aproximadamente 10 segundos en "correr" (usen
time.sleep(10) dentro de la task para simularlo). Construyan ese DAG desde cero, y midan cuánto
tiempo total tarda en completarse bajo tres configuraciones distintas de capacidad de worker.

---

## 1. Código del DAG (`dag_reportes_delivery.py`)

El siguiente código implementa las 6 tareas independientes solicitadas para simular la generación de reportes y un retardo artificial de 10 segundos por ciudad.

```python

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import time


def generar_reporte(ciudad):
    print(f"Iniciando reporte de {ciudad}...")

    # Simular procesamiento durante 10 segundos
    time.sleep(10)

    print(f"Reporte de {ciudad} terminado.")


with DAG(
    dag_id="ejercicio1",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ejercicio1", "delivery", "concurrencia"],
) as dag:

    ciudades = [
        "Lima",
        "Arequipa",
        "Trujillo",
        "Chiclayo",
        "Piura",
        "Cusco"
    ]

    for ciudad in ciudades:

        PythonOperator(
            task_id=f"reporte_{ciudad.lower()}",
            python_callable=generar_reporte,
            op_kwargs={"ciudad": ciudad},
        )
```

---

## 2. Tiempos Medidos de Ejecución

## A) worker_concurrency = 1
En esta configuración se demora entre 65 a 72 segundos 
<img width="761" height="243" alt="image" src="https://github.com/user-attachments/assets/372cb732-c62c-47c7-ae2a-31d01fb8d47a" />

<img width="922" height="388" alt="image" src="https://github.com/user-attachments/assets/9212bf0b-c24f-4d4d-81ef-ce41884e91b3" />


## B) worker_concurrency = 6
En esta configuración se demora entre 15 y 18 segundos 
<img width="774" height="141" alt="image" src="https://github.com/user-attachments/assets/6b0a8ec4-2b1f-4bc0-ae85-7a3d2b12caa1" />

<img width="916" height="386" alt="image" src="https://github.com/user-attachments/assets/3c1a0d2f-d6a8-4b32-accc-6eb4713a7e14" />



## C) worker_concurrency = 3
En esta configuración se demora entre 15 y 23 segundos 
<img width="1193" height="265" alt="image" src="https://github.com/user-attachments/assets/426b7122-5f05-4aa6-8280-f8d570e306c6" />

<img width="917" height="343" alt="image" src="https://github.com/user-attachments/assets/44107f25-e436-48ef-9b0d-7470723d8e6a" />



### Tabla de Resultados Obtenidos

| Configuración Evaluada | Workers Físicos | Concurrencia por Worker | Slots Totales | Rango de Tiempo Observado |
| :--- | :---: | :---: | :---: | :--- |
| **(a) Mínima Capacidad** | 1 | 1 | 1 | **65 a 72 segundos** |
| **(b) Escalado Vertical** | 1 | 6 | 6 | **15 a 18 segundos** |
| **(c) Escalado Horizontal**| 2 | 3 | 6 | **15 a 23 segundos** |

---

## 3. Análisis Técnico y Conclusión

### Explicación de los Resultados y Conclusión Corta
El escenario **(A)** es el más lento (65-72s) porque tiene un solo slot disponible, lo que obliga a procesar las 6 tareas de forma secuencial. En cambio, **(B)** y **(C)** bajan el tiempo drásticamente (15-23s) porque abren 6 slots simultáneos, permitiendo ejecutar todo en paralelo.La variación en (c) se debe al escalamiento horizontal: repartir tareas en dos contenedores genera latencia por comunicación de red y sincronización en Redis. Mientras el escalado vertical (b) es más rápido a nivel local, el horizontal (c) sacrifica microsegundos a cambio de aportar alta disponibilidad en producción.
