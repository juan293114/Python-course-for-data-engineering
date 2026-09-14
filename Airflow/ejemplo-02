# Caso Práctico Avanzado: Dispersión de Nóminas — Almacenes Éxito
**Curso:** PEDE/9 — Apache Airflow  
**Profesor:** Renato Arrascue  

Este repositorio documenta el diseño e implementación de un esquema de orquestación distribuida destinado a priorizar procesos financieros críticos y proteger las APIs de terceros de caídas por saturación.

---

## 1. Justificación del Negocio y Procesos Prioritarios (Celery Queues)
El pipeline gestiona el cálculo y transferencia de salarios de fin de mes para las cuatro sedes principales de Almacenes Éxito. Se segmentó la carga en dos niveles de prioridad mediante la cola Celery `nominas_criticas`:

* **`Bogotá` y `Medellín` (Prioridad Alta):** Representan los centros operativos y de distribución más masivos de la compañía. Cualquier retraso en la dispersión de fondos a los empleados de estas sedes genera un impacto social inmediato, huelgas operacionales y multas severas de los entes reguladores laborales.
* **`Cali` y `Barranquilla` (Prioridad Estándar / Cola Default):** Manejan volúmenes de personal considerablemente menores, por lo cual se enrutan a la cola por defecto para ser atendidos por la capacidad base de cómputo del clúster sin comprometer los tiempos de procesamiento de los nodos principales.

Al configurar el servicio de infraestructura `airflow-worker-nominas` para atender exclusivamente la cola `nominas_criticas`, se garantiza inmunidad arquitectónica: los salarios de Bogotá y Medellín jamás competirán en fila con reportes masivos de fondo u otras cargas menores del ecosistema Airflow.

---

## 2. Justificación Técnica de los Slots del Pool (Airflow Pools)
El recurso compartido y vulnerable externo es el **Gateway de la API corporativa de nuestro banco distribuidor**. Por motivos de infraestructura del sector bancario y mitigar ataques de denegación de servicios, el banco impone por contrato un límite estricto de **tasa de concurrencia máxima de 2 conexiones simultáneas por IP cliente**.

* **Criterio de Elección de 2 Slots:** Se crearon exactamente **2 slots** asignados al pool `pool_api_banco`. Si los flujos automáticos o ejecuciones manuales intentan invocar la tarea `dispersar_fondos_banco` de las cuatro ciudades al mismo tiempo, los mecanismos internos de Airflow bloquearán el paso de las dos tareas sobrantes, manteniéndolas en estado `queued`.
* **Beneficio de Protección:** En el instante en que cualquiera de las dos transferencias en curso libere su slot tras completar su ejecución, Airflow enviará de inmediato la siguiente tarea acumulada en la cola. Esto nos permite explotar al máximo el paralelismo permitido del canal bancario corporativo, blindando completamente a la infraestructura de Almacenes Éxito de recibir bloqueos de seguridad o errores de tipo `HTTP 429 Too Many Requests`.

