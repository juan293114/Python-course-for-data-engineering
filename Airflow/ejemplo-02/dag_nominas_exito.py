from airflow.decorators import dag, task
from datetime import datetime

FECHA_INICIO = datetime(2026, 9, 1)

# Estructura del negocio: Ciudades y nivel de prioridad para pago de nómina
SEDES_EXITO = [
    ("bogota", True),
    ("medellin", True),
    ("cali", False),
    ("barranquilla", False),
]


def crear_dag_nomina_exito(ciudad_id: str, es_prioritario: bool):
    # Enrutamiento de infraestructura Celery
    cola_asignada = "nominas_criticas" if es_prioritario else "default"

    @dag(
        dag_id=f"pago_nomina_exito_{ciudad_id}",
        schedule="0 5 30 * *",  # Corre los 30 de cada mes a las 5:00 AM
        start_date=FECHA_INICIO,
        catchup=False,
        tags=["exito", "finanzas", "celery", "colas_pools", cola_asignada],
    )
    def _dag():

        @task(queue=cola_asignada)
        def calcular_conceptos_nomina():
            print(f"[{ciudad_id.upper()}] Consolidando horas extra, recargos y novedades de empleados (cola={cola_asignada})")
            return {"ciudad": ciudad_id, "procesados": True}

        @task(queue=cola_asignada)
        def firmar_archivo_seguro(datos: dict):
            import time
            # Simula el empaquetado y cifrado criptográfico PGP del lote de empleados
            time.sleep(6)
            print(f"[{ciudad_id.upper()}] Archivo bancario firmado digitalmente.")
            return datos

        # pool="pool_api_banco" -- Mitiga que máximo 2 sedes ejecuten
        # dispersión bancaria concurrente en simultáneo sobre el gateway externo.
        @task(queue=cola_asignada, pool="pool_api_banco")
        def dispersar_fondos_banco(datos: dict):
            import time
            print(f"[{datos['ciudad'].upper()}] Abriendo canal seguro HTTPS con la API Bancaria...")
            time.sleep(7)  # Simula la ejecución batch de dispersión monetaria en la API
            print(f"[{datos['ciudad'].upper()}] Transferencias completadas de forma exitosa.")

        dispersar_fondos_banco(firmar_archivo_seguro(calcular_conceptos_nomina()))

    return _dag()


# Inicialización dinámica de los 4 pipelines independientes
for ciudad, prioritario in SEDES_EXITO:
    crear_dag_nomina_exito(ciudad, prioritario)
