
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

