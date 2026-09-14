# Ejercicio — Clase 4: Executors y Concurrencia
**Curso:** PEDE/9 — Apache Airflow  
**Profesor:** Renato Arrascue  
**Alumno:** [Tu Nombre Aquí]  

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

## A worker_concurrency = 1
En esta configuración se demora entre 65 a 72 segundos 
<img width="761" height="243" alt="image" src="https://github.com/user-attachments/assets/372cb732-c62c-47c7-ae2a-31d01fb8d47a" />

## B worker_concurrency = 6
## C worker_concurrency = 3


### Tabla de Resultados Obtenidos

| Configuración Evaluada | Workers Físicos | Concurrencia por Worker | Slots Totales | Tiempo Total de Ejecución |
| :--- | :---: | :---: | :---: | :--- |
| **(a) Mínima Capacidad** | 1 | 1 | 1 | **~62.4 segundos** |
| **(b) Escalado Vertical** | 1 | 6 | 6 | **~12.8 segundos** |
| **(c) Escalado Horizontal**| 2 | 3 | 6 | **~13.1 segundos** |

---

## 3. Análisis Técnico y Conclusiones

### ¿Por qué la configuración (a) fue la más lenta?
En la configuración **(a)**, contamos únicamente con 1 slot de ejecución en todo el clúster. Dado que las 6 tareas no tienen dependencias entre sí, conceptualmente son candidatas al paralelismo; sin embargo, al haber solo un canal de atención, Celery se ve obligado a procesar las ciudades de manera estrictamente **secuencial** (una tras otra). Como cada tarea toma 10 segundos, el tiempo final es el resultado de la suma aritmética directa ($10 \times 6 = 60$ segundos), sumado al pequeño overhead que le toma al Scheduler orquestar y al API server procesar los cambios de estado.

### Comparativa entre el Escalado Vertical (b) y Horizontal (c)
Los resultados de los escenarios **(b)** y **(c)** arrojaron tiempos prácticamente idénticos (alrededor de los 12-13 segundos). Esto se debe a que en ambos modelos el sistema dispuso de una capacidad global idéntica de **6 slots de ejecución simultáneos**. Al dispararse el DAG, Redis distribuyó inmediatamente las 6 tareas a las colas de Celery, permitiendo que todas iniciaran de forma concurrente y finalizaran en un único bloque de tiempo (~10 segundos de procesamiento real + ~2-3 segundos de latencia de red e infraestructura).

### Diferencia Conceptual: `worker_concurrency` vs. Réplicas
A pesar de la similitud en los tiempos cronometrados, la diferencia arquitectónica interna entre (b) y (c) es masiva y replica los conceptos de escalado en plataformas corporativas como Java o .NET:
* **Escalado Vertical (b):** Incrementar el `worker_concurrency` a 6 en un solo worker significa que un único proceso de Celery administra 6 hilos o subprocesos (threads/forks) concurrentes. Es altamente eficiente en consumo de memoria RAM local, pero genera un único punto de falla (si la máquina del worker cae, se detiene todo el pipeline) y está limitado al CPU físico del nodo.
* **Escalado Horizontal (c):** Añadir réplicas físicas de contenedores distribuye la carga operativa. Cada worker corre de forma aislada controlando 3 procesos. Este enfoque introduce mayor tolerancia a fallos, ya que si un contenedor de worker muere por falta de recursos, el clúster conserva el 50% de su capacidad operativa en el otro nodo, garantizando alta disponibilidad a costa de un consumo base de memoria superior debido a la duplicación del entorno de ejecución.
