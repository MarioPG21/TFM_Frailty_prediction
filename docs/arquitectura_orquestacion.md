# Arquitectura y funcionamiento de la orquestación — TFM Frailty Prediction

## Índice

1. [Visión general](#1-visión-general)
2. [Infraestructura Airflow](#2-infraestructura-airflow)
   - 2.1 [Servicios Docker (scheduler y apiserver)](#21-servicios-docker-scheduler-y-apiserver)
   - 2.2 [LocalExecutor — ejecución sin workers externos](#22-localexecutor--ejecución-sin-workers-externos)
   - 2.3 [JWT Secret — comunicación scheduler↔apiserver](#23-jwt-secret--comunicación-schedulerapiserver)
   - 2.4 [SimpleAuthManager — autenticación de la UI](#24-simpleauthmanager--autenticación-de-la-ui)
   - 2.5 [Python 3.11/3.12 — compatibilidad de versiones con Spark](#25-python-3113-12--compatibilidad-de-versiones-con-spark)
   - 2.6 [Variables de entorno clave](#26-variables-de-entorno-clave)
3. [Inicialización — `airflow-init`](#3-inicialización--airflow-init)
4. [DAG del reloj — `dags/clock.py`](#4-dag-del-reloj--dagsclockpy)
   - 4.1 [Tarea `advance_clock`](#41-tarea-advance_clock)
   - 4.2 [Tarea `publish_sources`](#42-tarea-publish_sources)
   - 4.3 [Tarea `confirm_clock`](#43-tarea-confirm_clock)
   - 4.4 [Operador `trigger_pipeline`](#44-operador-trigger_pipeline)
   - 4.5 [Dependencias entre tareas y atomicidad](#45-dependencias-entre-tareas-y-atomicidad)
5. [Scripts de publicación — `scripts/publish_*.py`](#5-scripts-de-publicación--scriptspublish_py)
   - 5.1 [Modo `--day` (usado por el reloj)](#51-modo---day-usado-por-el-reloj)
   - 5.2 [Modo `--ticks` (uso manual)](#52-modo---ticks-uso-manual)
   - 5.3 [Idempotencia y head_object](#53-idempotencia-y-head_object)
   - 5.4 [Fuentes y rutas en MinIO](#54-fuentes-y-rutas-en-minio)
6. [DAG del pipeline — `dags/pipeline.py`](#6-dag-del-pipeline--dagspipelinepy)
   - 6.1 [BashOperator sobre spark-submit](#61-bashoperator-sobre-spark-submit)
   - 6.2 [Comando spark-submit — flags y classpath](#62-comando-spark-submit--flags-y-classpath)
   - 6.3 [Cadena Bronze → Silver → Gold](#63-cadena-bronze--silver--gold)
   - 6.4 [max_active_runs=1 — exclusión mutua](#64-max_active_runs1--exclusión-mutua)
7. [Reloj de simulación — Variable `sim_day`](#7-reloj-de-simulación--variable-sim_day)
   - 7.1 [Diseño general](#71-diseño-general)
   - 7.2 [Límites de la cohorte](#72-límites-de-la-cohorte)
   - 7.3 [Estado inicial y primer tick](#73-estado-inicial-y-primer-tick)
   - 7.4 [Fin de cohorte — AirflowSkipException](#74-fin-de-cohorte--airflowskipexception)
8. [Flujo completo de extremo a extremo](#8-flujo-completo-de-extremo-a-extremo)
9. [Interacción entre los dos DAGs](#9-interacción-entre-los-dos-dags)
10. [Problemas resueltos durante la implementación](#10-problemas-resueltos-durante-la-implementación)
    - 10.1 [SparkSubmitOperator no se despacha en Airflow 3](#101-sparksubmitoperator-no-se-despacha-en-airflow-3)
    - 10.2 [Mismatch Python 3.11 / 3.12 en PySpark](#102-mismatch-python-3113-12-en-pyspark)
    - 10.3 [Contraseña de la UI — 401 Unauthorized](#103-contraseña-de-la-ui--401-unauthorized)
    - 10.4 [PATH_NOT_FOUND en Silver y Gold para fuentes opcionales](#104-path_not_found-en-silver-y-gold-para-fuentes-opcionales)
    - 10.5 [DAG pausado en primera creación](#105-dag-pausado-en-primera-creación)
    - 10.6 [Dos runs concurrentes bloqueándose mutuamente](#106-dos-runs-concurrentes-bloqueándose-mutuamente)
11. [Guía de operación](#11-guía-de-operación)
    - 11.1 [Primer arranque](#111-primer-arranque)
    - 11.2 [Lanzar un tick manualmente](#112-lanzar-un-tick-manualmente)
    - 11.3 [Activar avance automático](#113-activar-avance-automático)
    - 11.4 [Resetear la simulación](#114-resetear-la-simulación)
12. [Decisiones de diseño relevantes](#12-decisiones-de-diseño-relevantes)

---

## 1. Visión general

La capa de orquestación coordina dos responsabilidades distintas:

1. **Simular el paso del tiempo** — el pipeline trabaja con datos históricos sintéticos de una cohorte de 2024-01-01 a 2025-06-30. En lugar de esperar 18 meses reales, un reloj de simulación avanza día a día publicando en MinIO los datos que habrían llegado ese día.

2. **Ejecutar el pipeline Medallion** — en cada tick del reloj, los datos recién publicados se transforman a través de las capas Bronze → Silver → Gold usando Apache Spark.

Ambas responsabilidades se implementan como DAGs de Apache Airflow 3, comunicados a través de `TriggerDagRunOperator`:

```
┌─────────────────────────────────────────────────────────────────────┐
│  DAG: clock                                                         │
│                                                                     │
│  advance_clock ──► publish_sources ──► confirm_clock ──► trigger ──┼──►┐
│                                                                     │   │
└─────────────────────────────────────────────────────────────────────┘   │
                                                                          │  dispara
┌─────────────────────────────────────────────────────────────────────┐   │  y espera
│  DAG: pipeline                                                      │◄──┘
│                                                                     │
│  bronze ──► silver ──► gold                                         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Stack de orquestación:**

| Componente | Versión | Rol |
|---|---|---|
| Apache Airflow | 3.0.1 | Orquestador de DAGs |
| Airflow LocalExecutor | — | Ejecuta tareas como subprocesos del scheduler |
| PostgreSQL | 15 | Metastore de Airflow (DAGs, runs, variables, conexiones) |
| SimpleAuthManager | Airflow 3 nativo | Autenticación de la UI web |
| Apache Spark | 4.0.1 standalone | Motor de procesamiento distribuido |
| MinIO | RELEASE.2025-04-22 | Landing zone S3-compatible |

---

## 2. Infraestructura Airflow

### 2.1 Servicios Docker (scheduler y apiserver)

Airflow 3 separa sus funciones en dos procesos distintos que en este proyecto se ejecutan en dos contenedores Docker independientes:

**`airflow-scheduler`** — núcleo de Airflow. Responsabilidades:
- Parsear los ficheros DAG del directorio `/opt/airflow/dags`.
- Detectar cuándo una ejecución programada está lista.
- Despachar tareas al LocalExecutor (subprocesos).
- Con `AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR=False`, el dag-processor se integra dentro del proceso del scheduler, eliminando la necesidad de un tercer contenedor.

**`airflow-apiserver`** — interfaz pública. Responsabilidades:
- Servir la UI web en el puerto 8085 (mapeado al 8080 interno).
- Exponer la API REST v2.
- Servir la Execution API (`/execution/`) que el LocalExecutor usa para comunicar el estado de las tareas al scheduler.

```
Host: localhost:8085  →  airflow-apiserver:8080  →  Airflow UI + API REST
                                                 →  /execution/ (Execution API)

airflow-scheduler  ──[Execution API JWT]──►  airflow-apiserver:8080/execution/
```

**Por qué dos contenedores:** En Airflow 3 la Execution API es el canal por el que el scheduler comunica el inicio de tareas y recibe actualizaciones de estado. Si scheduler y apiserver compartieran el mismo proceso, un bloqueo del pipeline podría congelar también la UI. La separación permite que la interfaz web permanezca operativa durante ejecuciones pesadas de Spark.

### 2.2 LocalExecutor — ejecución sin workers externos

```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
```

`LocalExecutor` ejecuta cada tarea como un subproceso `fork()` dentro del contenedor `airflow-scheduler`. No requiere Redis, RabbitMQ, ni contenedores Celery adicionales.

Para un pipeline de un único usuario y volúmenes de datos moderados, `LocalExecutor` es la opción correcta porque:
- El overhead es mínimo (sin serialización de mensajes, sin brokers).
- Los subprocesos heredan el entorno del scheduler (variables de entorno, rutas de Python).
- La paralelización es suficiente: las tres tareas del pipeline (`bronze`, `silver`, `gold`) se ejecutan secuencialmente por diseño, no en paralelo.

El único límite es que el scheduler y las tareas comparten los recursos del mismo contenedor. Si se quisiera escalar a múltiples pipelines concurrentes pesados, habría que migrar a `CeleryExecutor`.

### 2.3 JWT Secret — comunicación scheduler↔apiserver

```yaml
AIRFLOW__API_AUTH__JWT_SECRET: "${AIRFLOW_JWT_SECRET}"
```

El `LocalExecutor` de Airflow 3 llama a la Execution API del `airflow-apiserver` para comunicar el inicio y el resultado de cada tarea. Esas llamadas están firmadas con un JWT (JSON Web Token) usando la clave `AIRFLOW_JWT_SECRET`.

**Sin esta clave compartida**, cada contenedor generaría una clave aleatoria diferente en el arranque. El scheduler obtendría `403 Forbidden` al intentar ejecutar tareas porque el apiserver rechazaría su token. Las tareas quedarían perpetuamente en estado `queued` sin progresión ni error visible.

La clave se genera una sola vez y se almacena en el fichero `.env`:

```bash
AIRFLOW_JWT_SECRET=$(python -c "import secrets; print(secrets.token_hex(32))")
```

### 2.4 SimpleAuthManager — autenticación de la UI

Airflow 3 reemplaza Flask-AppBuilder (FAB) con `SimpleAuthManager`, un sistema de autenticación minimalista incluido de serie.

**Cómo funciona:**

1. El servicio `airflow-init` ejecuta `docker/init_auth.py`, que escribe el fichero de contraseñas:

```python
# docker/init_auth.py
import json, os

user = os.environ.get("AIRFLOW_WWW_USER", "airflow")
pwd  = os.environ.get("AIRFLOW_WWW_PASSWORD", "airflow")
path = os.path.join(
    os.environ.get("AIRFLOW_HOME", "/opt/airflow"),
    "simple_auth_manager_passwords.json.generated",
)
with open(path, "w") as f:
    json.dump({user: pwd}, f)
```

El fichero resultante es un JSON plano:

```json
{"airflow": "airflow"}
```

2. Airflow lee ese fichero al arrancar, configurado mediante:

```yaml
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE: "/opt/airflow/simple_auth_manager_passwords.json.generated"
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS: "airflow:admin"
```

3. Al hacer login, `SimpleAuthManager` compara la contraseña recibida con el valor en el JSON mediante **comparación de texto plano** (`passwords[username] == body.password`). No usa bcrypt ni ningún hash.

**Aspectos críticos:**
- La clave de configuración es `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE` (sección `core`, no `simple_auth_manager`). Una sección incorrecta hace que Airflow ignore el fichero y rechace cualquier contraseña.
- El path debe estar bajo `$AIRFLOW_HOME` (`/opt/airflow/`), no en subdirectorios como `config/`. Airflow solo busca el fichero en la ruta exacta configurada.
- La contraseña es texto plano, no hash. No intentar escribir un hash bcrypt: el login fallará con 401.

### 2.5 Python 3.11/3.12 — compatibilidad de versiones con Spark

PySpark exige que el driver Python y los executors (workers Spark) usen exactamente la misma versión menor de Python. Una diferencia de versión produce un error de serialización al enviar closures entre el driver y los workers:

```
Exception: Python in worker has different version 3.11 than that in driver 3.12
```

En este proyecto:
- Los contenedores `spark-worker-1` y `spark-worker-2` ejecutan **Python 3.11** (imagen `python:3.11` base de `dockerfile-spark`).
- El contenedor `airflow-scheduler` base usa **Python 3.12** (imagen oficial de Airflow 3).

La solución es instalar PySpark y Delta-Spark para Python 3.11 dentro del contenedor de Airflow, en un directorio aislado, y apuntar el driver a ese intérprete:

```dockerfile
# docker/dockerfile-airflow (fragmento)
RUN mkdir -p /opt/py311-packages && \
    curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py && \
    python3.11 /tmp/get-pip.py --target=/opt/py311-packages && \
    PYTHONPATH=/opt/py311-packages python3.11 /opt/py311-packages/pip \
        install --target=/opt/py311-packages --no-cache-dir \
        "pyspark==4.0.1" "delta-spark==4.0.0" && \
    rm /tmp/get-pip.py
```

```yaml
# docker-compose.yml (airflow-common env)
PYSPARK_DRIVER_PYTHON: "python3.11"       # el driver usa python3.11
PYTHONPATH: "/opt/py311-packages"         # donde están pyspark y delta para 3.11
SPARK_HOME: "/home/airflow/.local/lib/python3.12/site-packages/pyspark"
```

`SPARK_HOME` sigue apuntando a la instalación de python3.12 porque solo se usa para localizar el binario `spark-submit` (que es un script de shell, independiente de la versión Python). El driver y los imports de PySpark van por `PYTHONPATH`/`python3.11`.

### 2.6 Variables de entorno clave

| Variable | Valor | Propósito |
|---|---|---|
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | Ejecutar tareas como subprocesos del scheduler |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://...` | Metastore de Airflow |
| `AIRFLOW__CORE__FERNET_KEY` | desde `.env` | Cifrar credenciales almacenadas en postgres |
| `AIRFLOW__API_AUTH__JWT_SECRET` | desde `.env` | Firmar llamadas Execution API |
| `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` | `airflow:admin` | Usuario:rol de la UI |
| `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE` | `/opt/airflow/simple_auth_manager_passwords.json.generated` | Path del fichero de contraseñas |
| `PYSPARK_DRIVER_PYTHON` | `python3.11` | Intérprete Python del driver Spark |
| `PYTHONPATH` | `/opt/py311-packages` | Paquetes PySpark/Delta para python3.11 |
| `SPARK_HOME` | `/home/airflow/.local/lib/python3.12/site-packages/pyspark` | Localización del binario `spark-submit` |
| `AIRFLOW_CONN_SPARK_DEFAULT` | `{"conn_type":"spark","host":"spark://spark-master","port":7077}` | Conexión al cluster Spark |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | desde `.env` | Credenciales MinIO propagadas a procesos Spark |
| `MINIO_ENDPOINT` | `http://minio:9000` | Endpoint S3 dentro de la red Docker |

---

## 3. Inicialización — `airflow-init`

Antes de que el scheduler y el apiserver arranquen, se ejecuta un servicio one-shot que prepara el entorno:

```yaml
airflow-init:
  entrypoint: /bin/bash
  command:
    - -c
    - |
      mkdir -p /opt/airflow/{logs,dags,plugins,config}
      airflow db migrate
      python3 /opt/airflow/init_auth.py
      echo "Inicialización de Airflow completada."
  user: "0:0"    # root para tener permisos de escritura
  restart: "no"
```

Tres pasos en orden:

1. **`mkdir -p`** — crea los directorios que Airflow espera encontrar. Sin esto, el arranque falla porque Airflow intenta escribir logs antes de que existan los directorios.

2. **`airflow db migrate`** — aplica las migraciones del esquema SQL al PostgreSQL. En el primer arranque crea todas las tablas. En arranques posteriores es un no-op si el esquema ya está actualizado.

3. **`python3 /opt/airflow/init_auth.py`** — escribe el fichero de contraseñas de `SimpleAuthManager` en `/opt/airflow/simple_auth_manager_passwords.json.generated`.

El scheduler y el apiserver tienen `depends_on: airflow-init: condition: service_completed_successfully`, así que no arrancan hasta que `airflow-init` termina con código de salida 0.

---

## 4. DAG del reloj — `dags/clock.py`

El reloj es un DAG de cuatro pasos que representa un "tick" de simulación: avanza el tiempo un día, publica los datos de ese día, confirma el avance, y lanza el pipeline de transformación.

```python
@dag(
    dag_id="clock",
    schedule=None,          # sin schedule: disparo manual o cron externo
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["orchestration", "simulation"],
)
def clock_dag():
    next_day = advance_clock()
    pub = publish_sources(next_day)
    conf = confirm_clock(next_day)
    pub >> conf >> trigger_pipeline
```

**Grafo de dependencias:**

```
advance_clock
     │
     ▼
publish_sources ──► confirm_clock ──► trigger_pipeline
```

`advance_clock` pasa `next_day` a `publish_sources` y a `confirm_clock` mediante XCom (el mecanismo de intercambio de valores entre tareas de Airflow). Cada función marcada con `@task` recibe el retorno de `advance_clock()` como argumento.

### 4.1 Tarea `advance_clock`

```python
@task
def advance_clock() -> str:
    sim_day_str = Variable.get("sim_day", default_var=_BEFORE_START)
    sim_day = date.fromisoformat(sim_day_str)
    if sim_day >= COHORT_END:
        raise AirflowSkipException(f"Cohorte completada: {sim_day} >= {COHORT_END}")
    next_day = sim_day + timedelta(days=1)
    print(f"Reloj: {sim_day} → {next_day}")
    return next_day.isoformat()
```

Lee la Variable de Airflow `sim_day` del metastore PostgreSQL. Si no existe, usa `2023-12-31` (el día anterior al inicio de la cohorte), de modo que el primer tick produzca `2024-01-01`.

Calcula `next_day = sim_day + 1 día` y lo retorna como cadena ISO (YYYY-MM-DD). **Todavía no escribe la Variable**: el avance no se confirma hasta que `publish_sources` termine con éxito. Esto garantiza que si la publicación de datos falla, el reloj no avanza y el siguiente tick reintentará el mismo día.

### 4.2 Tarea `publish_sources`

```python
@task
def publish_sources(next_day: str) -> None:
    for script in PUBLISH_SCRIPTS:
        result = subprocess.run(
            [sys.executable, script, "--day", next_day],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Publicación fallida [{script}]:\n{result.stderr}")
```

Ejecuta los cinco scripts de publicación secuencialmente como subprocesos Python. Usa `sys.executable` (el mismo intérprete del scheduler) para heredar el entorno y el `PYTHONPATH`.

`PUBLISH_SCRIPTS` contiene, en orden:
1. `publish_clinical.py` — registros clínicos CSV (Fuente A)
2. `publish_sppb.py` — cuestionarios SPPB JSONL (Fuente B1)
3. `publish_lifestyle.py` — cuestionarios de estilo de vida JSONL (Fuente B2)
4. `publish_gait.py` — telemetría de marcha JSONL (Fuente C)
5. `publish_labels.py` — etiquetas de fragilidad CSV

Si cualquier script retorna código de salida distinto de 0, la tarea falla y se lanza `RuntimeError` con el stderr del script. Esto impide que `confirm_clock` avance el reloj con datos incompletos.

Timeout de 120 segundos por script: suficiente para subir un fichero mensual a MinIO en red local, y suficientemente restrictivo para detectar cuelgues de boto3.

### 4.3 Tarea `confirm_clock`

```python
@task
def confirm_clock(next_day: str) -> None:
    Variable.set("sim_day", next_day)
    print(f"sim_day confirmado: {next_day}")
```

Escribe el nuevo valor de `sim_day` en el metastore de Airflow (PostgreSQL). Solo se ejecuta si `publish_sources` terminó con éxito (porque `pub >> conf` en el grafo de dependencias). Esta separación entre calcular el día siguiente (`advance_clock`) y confirmarlo (`confirm_clock`) es la garantía de atomicidad del reloj.

### 4.4 Operador `trigger_pipeline`

```python
trigger_pipeline = TriggerDagRunOperator(
    task_id="trigger_pipeline",
    trigger_dag_id="pipeline",
    wait_for_completion=True,
    poke_interval=30,
)
```

`TriggerDagRunOperator` con `wait_for_completion=True` dispara el DAG `pipeline` y bloquea hasta que termina. El scheduler consulta el estado del pipeline cada `poke_interval=30` segundos. Si el pipeline falla, `trigger_pipeline` también falla, marcando el tick del reloj como fallido (aunque `sim_day` ya se habrá confirmado).

**Por qué esperar:** Si el reloj no esperara al pipeline, podría disparar el siguiente tick antes de que el pipeline del tick anterior terminara. Dos runs concurrentes del pipeline compiten por los mismos recursos Spark y pueden corromperse mutuamente (el segundo run leería Bronze a medias mientras el primero lo está escribiendo).

### 4.5 Dependencias entre tareas y atomicidad

```
advance_clock ──► publish_sources ──► confirm_clock ──► trigger_pipeline
```

La dependencia `advance_clock → publish_sources` garantiza que los datos existen antes de confirmar. La dependencia `publish_sources → confirm_clock` garantiza que el reloj solo avanza si los datos se publicaron correctamente. La dependencia `confirm_clock → trigger_pipeline` garantiza que el pipeline se lanza con el reloj ya actualizado.

Si el pipeline falla después de que `confirm_clock` haya terminado, el día simulado ya está avanzado. En el siguiente tick, `advance_clock` avanzará un día más. El pipeline recogerá todos los datos acumulados porque la capa Bronze usa **watermarks incrementales** por fuente: procesa todos los ficheros de MinIO que tengan una fecha de modificación posterior al último watermark registrado, independientemente del número de días que hayan transcurrido entre ejecuciones.

---

## 5. Scripts de publicación — `scripts/publish_*.py`

Existen cinco scripts, uno por fuente de datos, con una interfaz común:

| Script | Fuente | Formato | Ruta en MinIO |
|---|---|---|---|
| `publish_clinical.py` | Fuente A — registros clínicos | CSV | `landing/clinical/YYYY/MM/` |
| `publish_sppb.py` | Fuente B1 — cuestionarios SPPB | JSONL | `landing/sppb/YYYY/MM/` |
| `publish_lifestyle.py` | Fuente B2 — estilo de vida | JSONL | `landing/lifestyle/YYYY/MM/` |
| `publish_gait.py` | Fuente C — telemetría de marcha | JSONL | `landing/gait/YYYY/MM/` |
| `publish_labels.py` | Etiquetas de fragilidad | CSV | `landing/labels/YYYY/MM/` |

Los datos sintéticos están organizados por mes: cada fichero tiene el nombre `YYYY-MM.csv` o `YYYY-MM.jsonl` y contiene todos los registros de ese mes calendario.

### 5.1 Modo `--day` (usado por el reloj)

```python
parser.add_argument("--day", default=None,
    help="Día simulado YYYY-MM-DD: sube el mes que contiene ese día (idempotente)")
```

Cuando el reloj llama al script con `--day 2024-01-15`, el script:

1. Extrae el año y mes del día simulado (`2024-01`).
2. Busca el fichero correspondiente en `synthetic_data/source_a/clinical_records/2024-01.csv`.
3. Comprueba si el objeto ya existe en MinIO (`head_object`).
4. Si no existe, lo sube. Si ya existe, imprime `"ya publicado, omitiendo"` y termina.

Esto implementa la idempotencia: si el mismo tick se reintenta (por fallo posterior), el script no sube el fichero dos veces.

Si no existe fichero para ese mes (por ejemplo, SPPB empieza en junio de 2024), imprime `"sin datos para este mes"` y termina sin error. Esto permite que todos los scripts se llamen en todos los ticks sin que los ticks tempranos fallen por fuentes que todavía no tienen datos.

### 5.2 Modo `--ticks` (uso manual)

```bash
python publish_clinical.py --ticks 1      # sube el primer mes pendiente
python publish_clinical.py --ticks all    # sube todos los meses pendientes
python publish_clinical.py --ticks all --delay 5  # con pausa entre meses
```

Este modo existe para uso manual fuera del reloj: permite poblar MinIO con varios meses de una sola vez sin pasar por el DAG. Es útil para pruebas o para cargar la cohorte completa de golpe.

### 5.3 Idempotencia y head_object

```python
def _exists(client, bucket, key):
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False
```

`head_object` hace una petición HTTP HEAD al objeto S3: retorna 200 si existe, 404 si no. Es mucho más eficiente que listar el bucket completo (`list_objects_v2`), que en un bucket con miles de objetos implicaría paginar todas las páginas antes de poder determinar si un objeto concreto existe.

### 5.4 Fuentes y rutas en MinIO

```
landing/
├── clinical/
│   └── 2024/
│       ├── 01/  ← 2024-01.csv
│       ├── 02/  ← 2024-02.csv
│       └── ...
├── gait/
│   └── 2024/
│       ├── 01/  ← 2024-01.jsonl
│       └── ...
├── sppb/
│   └── 2024/
│       ├── 06/  ← 2024-06.jsonl  (primer mes disponible)
│       └── ...
├── lifestyle/
│   └── 2024/
│       └── ...
└── labels/
    └── 2024/
        └── ...
```

La ruta incluye el año y el mes como niveles de directorio (`YYYY/MM/`) para facilitar el filtrado por prefix en Spark: `spark.read.format("csv").load("s3a://landing/clinical/2024/01/")`.

---

## 6. DAG del pipeline — `dags/pipeline.py`

El pipeline transforma los datos de MinIO a través de las tres capas Medallion. Se dispara desde el reloj o manualmente desde la UI.

```python
@dag(
    dag_id="pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline", "medallion"],
)
def pipeline_dag():
    bronze = BashOperator(task_id="bronze", bash_command=f"{_SUBMIT} bronze", ...)
    silver = BashOperator(task_id="silver", bash_command=f"{_SUBMIT} silver", ...)
    gold   = BashOperator(task_id="gold",   bash_command=f"{_SUBMIT} gold",   ...)
    bronze >> silver >> gold
```

### 6.1 BashOperator sobre spark-submit

Cada tarea llama a `spark-submit` directamente mediante `BashOperator`. Esta elección reemplaza a `SparkSubmitOperator`, que en Airflow 3 no funciona correctamente (ver sección 10.1).

`BashOperator` de Airflow 3 debe importarse desde el proveedor estándar:

```python
from airflow.providers.standard.operators.bash import BashOperator
```

El import `from airflow.operators.bash import BashOperator` (ruta antigua) está deprecado en Airflow 3 y genera un warning.

### 6.2 Comando spark-submit — flags y classpath

```python
_SUBMIT = (
    "$SPARK_HOME/bin/spark-submit"
    " --master spark://spark-master:7077"
    " --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
    " --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    " --conf spark.driver.extraClassPath=/opt/spark/jars/*"
    " --conf spark.executor.extraClassPath=/opt/spark/jars/*"
    " /opt/spark-apps/scripts/run_layer.py"
)
```

Cada flag tiene un propósito específico:

| Flag | Propósito |
|---|---|
| `--master spark://spark-master:7077` | Modo standalone: el driver se conecta al master Spark |
| `spark.sql.extensions=...DeltaSparkSessionExtension` | Registra las extensiones SQL de Delta Lake en la SparkSession |
| `spark.sql.catalog.spark_catalog=...DeltaCatalog` | Sustituye el catálogo por defecto por el de Delta, habilitando `MERGE INTO` y time-travel |
| `spark.driver.extraClassPath=/opt/spark/jars/*` | JAR de Delta Lake y S3A en el classpath del driver |
| `spark.executor.extraClassPath=/opt/spark/jars/*` | JAR de Delta Lake y S3A en el classpath de los executors (workers) |

El directorio `/opt/spark/jars/` está montado en todos los contenedores (Airflow y Spark) y contiene los JARs de Delta Lake, Hadoop S3A y sus dependencias transitivas. Esto elimina la necesidad de `--packages` (que descargaría los JARs en cada ejecución desde Maven Central).

El script `run_layer.py` recibe el argumento posicional (`bronze`, `silver` o `gold`) e invoca el módulo correspondiente:

```python
# scripts/run_layer.py (simplificado)
import sys
from pipeline.spark_session import get_spark

layer = sys.argv[1]   # "bronze" | "silver" | "gold"
spark = get_spark(f"pipeline-{layer}")

if layer == "bronze":
    from pipeline.bronze import run_all; run_all(spark)
elif layer == "silver":
    from pipeline.silver import run_all; run_all(spark)
elif layer == "gold":
    from pipeline.gold import run_all; run_all(spark)

spark.stop()
```

### 6.3 Cadena Bronze → Silver → Gold

```
BashOperator(bronze) ──► BashOperator(silver) ──► BashOperator(gold)
```

La dependencia estricta `bronze >> silver >> gold` garantiza que cada capa lee datos completamente escritos por la capa anterior. Delta Lake ofrece consistencia transaccional (lecturas atómicas), pero la dependencia explícita en Airflow añade una segunda barrera: `silver` ni siquiera arranca hasta que `bronze` ha terminado con éxito.

Cada capa es **idempotente** por diseño:
- **Bronze**: escribe con `MERGE INTO` por clave natural (`patient_id` + fecha de fuente). Si se ejecuta dos veces con los mismos datos de landing, el segundo MERGE no añade filas duplicadas.
- **Silver**: escribe con `MERGE INTO` por `patient_id` + timestamp de ingesta.
- **Gold**: escribe con `mode("overwrite")` — recalcula la tabla de entrenamiento completa sobre los datos Silver actuales.

### 6.4 max_active_runs=1 — exclusión mutua

```python
@dag(..., max_active_runs=1)
```

Solo puede haber un run activo del DAG `pipeline` a la vez. Si el reloj dispara un segundo tick mientras el pipeline del primero aún está corriendo, el segundo `TriggerDagRunOperator` espera hasta que el primero termine (porque `wait_for_completion=True` hace que el reloj ya espere). En el caso de disparos manuales desde la UI, `max_active_runs=1` impide que se acumulen runs concurrentes que compitan por los mismos recursos Delta Lake.

---

## 7. Reloj de simulación — Variable `sim_day`

### 7.1 Diseño general

La Variable de Airflow `sim_day` es el único estado persistente del reloj. Se almacena en el metastore PostgreSQL y es legible y modificable desde la UI web (Admin → Variables).

```
Estado en PostgreSQL:
  key:   "sim_day"
  value: "2024-01-15"   ← cadena ISO 8601, YYYY-MM-DD
```

Cada tick del reloj:
1. Lee `sim_day` al inicio (en `advance_clock`).
2. Calcula `sim_day + 1 día`.
3. Publica los datos de ese día en MinIO.
4. Escribe el nuevo valor en `sim_day` (en `confirm_clock`).

El valor de `sim_day` representa **el último día ya procesado**. Cuando `sim_day = 2024-01-15`, significa que todos los datos hasta e incluyendo el 15 de enero de 2024 han sido publicados y procesados.

### 7.2 Límites de la cohorte

```python
COHORT_START = date(2024, 1, 1)
COHORT_END   = date(2025, 6, 30)
```

La cohorte abarca 547 días (18 meses). Los datos sintéticos se generaron para este rango. Fuera de este rango no hay ficheros en `synthetic_data/`, por lo que los scripts de publicación devolverían `"sin datos para este mes"`.

El reloj avanza día a día para respetar la temporalidad de los datos: no se puede "ver" el resultado de un análisis de sangre del día 15 si solo se han procesado datos hasta el día 10. Esta restricción es especialmente importante para las etiquetas de fragilidad, que tienen una fecha de confirmación (`label_available_date`) que puede ser varios días posterior a la fecha de toma de la muestra.

### 7.3 Estado inicial y primer tick

Si la Variable `sim_day` no existe en PostgreSQL (primer arranque tras un reset), `Variable.get` devuelve el valor por defecto:

```python
_BEFORE_START = (COHORT_START - timedelta(days=1)).isoformat()  # "2023-12-31"

sim_day_str = Variable.get("sim_day", default_var=_BEFORE_START)
```

Con `sim_day = 2023-12-31`, el primer tick calcula `next_day = 2024-01-01` y publica los datos de enero de 2024. Tras el primer tick, `sim_day = 2024-01-01`.

### 7.4 Fin de cohorte — AirflowSkipException

```python
if sim_day >= COHORT_END:
    raise AirflowSkipException(f"Cohorte completada: {sim_day} >= {COHORT_END}")
```

`AirflowSkipException` marca la tarea (y por propagación, todo el DAG) como `skipped` en lugar de `failed`. Desde la UI, una ejecución skipped es visible y tiene color naranja, lo que permite distinguir "la cohorte terminó normalmente" de "algo falló". Si se usara un `return` normal, el DAG completaría con éxito pero también ejecutaría `publish_sources` (que no haría nada) y `trigger_pipeline` (que lanzaría un run vacío de pipeline). La excepción corta el DAG limpiamente.

---

## 8. Flujo completo de extremo a extremo

Un tick completo del sistema, desde el disparo del reloj hasta la tabla Gold actualizada:

```
Usuario (UI Airflow o cron)
  │
  │  Trigger manual del DAG "clock"
  ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK: advance_clock                                                │
│  Lee sim_day="2024-01-01" de PostgreSQL                             │
│  Calcula next_day="2024-01-02"                                      │
│  Retorna "2024-01-02" via XCom                                      │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK: publish_sources(next_day="2024-01-02")                       │
│                                                                     │
│  python3 publish_clinical.py --day 2024-01-02                       │
│    → stem="2024-01" → key="clinical/2024/01/2024-01.csv"           │
│    → head_object → ya existe → "omitiendo"  (idempotente)           │
│                                                                     │
│  python3 publish_sppb.py --day 2024-01-02                           │
│    → stem="2024-01" → sin fichero para enero → "sin datos"          │
│                                                                     │
│  python3 publish_lifestyle.py --day 2024-01-02  → "sin datos"       │
│                                                                     │
│  python3 publish_gait.py --day 2024-01-02                           │
│    → stem="2024-01" → ya existe → "omitiendo"                       │
│                                                                     │
│  python3 publish_labels.py --day 2024-01-02                         │
│    → stem="2024-01" → ya existe → "omitiendo"                       │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK: confirm_clock(next_day="2024-01-02")                         │
│  Variable.set("sim_day", "2024-01-02") en PostgreSQL                │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK: trigger_pipeline                                             │
│  Dispara DAG "pipeline" y espera (poke cada 30s)                    │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          │  DAG "pipeline" ejecutado en paralelo
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK: bronze                                                       │
│  spark-submit run_layer.py bronze                                   │
│    → Lee landing/clinical/2024/01/*.csv (watermark: sin cambios)    │
│    → Lee landing/gait/2024/01/*.jsonl   (watermark: sin cambios)    │
│    → Lee landing/labels/2024/01/*.csv   (watermark: sin cambios)    │
│    → Escribe MERGE en bronze/clinical, bronze/gait, bronze/labels   │
│    → sppb y lifestyle: sin ficheros → skip                          │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK: silver                                                       │
│  spark-submit run_layer.py silver                                   │
│    → Lee bronze/clinical → valida → MERGE silver/clinical           │
│    → Lee bronze/gait     → valida → MERGE silver/gait               │
│    → Lee bronze/labels   → valida → MERGE silver/labels             │
│    → bronze/sppb: PATH_NOT_FOUND → skip (fuente no activa aún)     │
│    → bronze/lifestyle:   PATH_NOT_FOUND → skip                      │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK: gold                                                         │
│  spark-submit run_layer.py gold                                     │
│    → Lee silver/clinical (base del join)                            │
│    → Lee gold/gait_features (as-of join por session_date)           │
│    → silver/sppb: PATH_NOT_FOUND → DataFrame vacío (LEFT JOIN)     │
│    → silver/lifestyle: PATH_NOT_FOUND → DataFrame vacío (LEFT JOIN) │
│    → Lee silver/labels → as-of join con anti-leakage               │
│      (label_available_date <= snapshot_date)                        │
│    → Escribe overwrite en gold/training                             │
│    → 43 filas × 38 columnas, frailty_label=NULL (enero 2024)       │
└─────────────────────────────────────────────────────────────────────┘
```

Duración observada del pipeline completo (un tick, datos de enero 2024):
- `bronze`: ~35 segundos
- `silver`: ~70 segundos
- `gold`: ~59 segundos
- **Total**: ~2,5 minutos

---

## 9. Interacción entre los dos DAGs

Los dos DAGs son independientes en cuanto a schedule (`schedule=None` en ambos), pero están acoplados a través de `TriggerDagRunOperator`. Este diseño permite:

1. **Disparo manual del pipeline** sin pasar por el reloj: útil para depuración o para reejecutar una transformación sin publicar nuevos datos.

2. **Disparo manual del reloj** desde la UI: un clic en "Trigger DAG" en el DAG `clock` ejecuta un tick completo.

3. **Schedule automático del reloj**: cambiar `schedule=None` a un cron expression como `"*/5 * * * *"` en `clock.py` hace que el sistema avance automáticamente un día cada 5 minutos reales (18 meses de cohorte ≈ 3 días y 2 horas de simulación).

```
Interacción entre DAGs:

clock.trigger_pipeline
  │
  │  TriggerDagRunOperator(wait_for_completion=True)
  │  ─────────────────────────────────────────────►  pipeline (nuevo run)
  │                                                       │
  │  poke cada 30s                                        │ bronze→silver→gold
  │  ◄─────────────────────────────────────────────────── │ (estado: running)
  │                                                       │
  │  run completado                                       │
  │  ◄──────────────────────────────────────────────────  pipeline (success/failed)
  │
  │  trigger_pipeline: success o failed según el pipeline
  ▼
clock run termina
```

---

## 10. Problemas resueltos durante la implementación

### 10.1 SparkSubmitOperator no se despacha en Airflow 3

**Síntoma:** Las tareas del DAG `pipeline` se quedaban indefinidamente en estado `queued` sin progresar ni mostrar error.

**Causa raíz:** Airflow 3 introdujo un nuevo modelo de ejecución basado en la Execution API. El LocalExecutor llama al apiserver para obtener `StartupDetails` (la definición de la tarea a ejecutar). `SparkSubmitOperator` de `apache-airflow-providers-apache-spark==4.11.0` no implementa correctamente la interfaz del nuevo Task SDK y nunca recibe los `StartupDetails`. El resultado es una tarea que el scheduler marca como `queued` pero que el executor nunca llega a lanzar.

El comando `airflow tasks test pipeline bronze` funcionaba porque ese modo de prueba ejecuta la tarea directamente, sin pasar por el scheduler ni por la Execution API.

**Solución:** Reemplazar `SparkSubmitOperator` por `BashOperator` llamando a `spark-submit` directamente:

```python
# Antes (no funciona en Airflow 3)
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
bronze = SparkSubmitOperator(
    task_id="bronze",
    application="/opt/spark-apps/scripts/run_layer.py",
    application_args=["bronze"],
    conn_id="spark_default",
    ...
)

# Después (funciona en Airflow 3)
from airflow.providers.standard.operators.bash import BashOperator
bronze = BashOperator(
    task_id="bronze",
    bash_command=f"{_SUBMIT} bronze",
    execution_timeout=timedelta(minutes=30),
)
```

`BashOperator` sí implementa el nuevo Task SDK de Airflow 3 y se despacha correctamente.

### 10.2 Mismatch Python 3.11/3.12 en PySpark

**Síntoma:** `spark-submit` desde el contenedor Airflow fallaba con:

```
Exception: Python in worker has different version 3.11 than that in driver 3.12
Traceback: ... pyspark/serializers.py ... PicklingError
```

**Causa raíz:** La imagen oficial de Airflow 3 usa Python 3.12. Los workers Spark usan Python 3.11 (la imagen base del `dockerfile-spark`). PySpark requiere versiones idénticas porque serializa funciones Python (UDFs, lambdas) con `pickle`, y el protocolo de pickle puede ser incompatible entre versiones menores.

**Solución:** Instalar PySpark 4.0.1 y Delta-Spark 4.0.0 para Python 3.11 en un directorio separado dentro del contenedor Airflow (`/opt/py311-packages`) y configurar el driver para usar ese intérprete:

```yaml
PYSPARK_DRIVER_PYTHON: "python3.11"
PYTHONPATH: "/opt/py311-packages"
```

El binario `spark-submit` no cambia (sigue siendo el de la instalación de python3.12) porque es un script de shell que no depende de la versión Python.

### 10.3 Contraseña de la UI — 401 Unauthorized

**Síntoma:** El login en `http://localhost:8085` con las credenciales `airflow/airflow` devolvía 401 Unauthorized.

**Causa raíz:** Tres problemas simultáneos:

1. `init_auth.py` escribía el fichero en `/opt/airflow/config/simple_auth_manager_passwords.json.generated` (ruta incorrecta). La ruta correcta es `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated` = `/opt/airflow/simple_auth_manager_passwords.json.generated`.

2. La variable de entorno configuraba la sección incorrecta: `AIRFLOW__SIMPLE_AUTH_MANAGER__PASSWORDS_FILE` (que Airflow ignora). La correcta es `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE`.

3. En un intento de fix intermedio se escribió un hash bcrypt en el fichero. `SimpleAuthManager` de Airflow 3 compara contraseñas en **texto plano** (`passwords[username] == body.password`), no descifra hashes.

**Solución:**
- Corregir el path en `init_auth.py` para usar `$AIRFLOW_HOME`.
- Corregir la sección en `docker-compose.yml`: `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE`.
- Escribir la contraseña en texto plano (sin hash).

### 10.4 PATH_NOT_FOUND en Silver y Gold para fuentes opcionales

**Síntoma:** La capa Silver fallaba con `AnalysisException: PATH_NOT_FOUND: s3a://bronze/sppb` en ticks de enero-mayo de 2024, porque SPPB solo tiene datos desde junio de 2024.

**Causa raíz:** Las fuentes SPPB y lifestyle no tienen datos en todos los meses de la cohorte. En enero de 2024 no existen ficheros en MinIO para esas fuentes, por lo que el script `publish_sppb.py --day 2024-01-01` imprime `"sin datos para este mes"` y no sube nada. Bronze intenta leer `s3a://bronze/sppb` y lanza `PATH_NOT_FOUND` porque la tabla Delta ni siquiera existe.

**Solución en Silver:** Guard try/except en `transform_sppb.py` y `transform_lifestyle.py`:

```python
def run(spark: SparkSession) -> None:
    try:
        df = spark.read.format("delta").load(BRONZE.SPPB)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            print("[sppb] Silver: sin datos en bronze todavía, omitiendo")
            return
        raise
```

**Solución en Gold:** Helper `_read_or_empty` que devuelve un DataFrame vacío con esquema mínimo cuando la tabla no existe:

```python
def _read_or_empty(spark, path, pid_col, date_col):
    try:
        return spark.read.format("delta").load(path)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            schema = StructType([
                StructField(pid_col,  StringType(),    True),
                StructField(date_col, TimestampType(), True),
            ])
            return spark.createDataFrame([], schema)
        raise
```

El LEFT JOIN de `_asof_join_latest` con un DataFrame vacío simplemente no añade columnas de SPPB/lifestyle para esos pacientes, que quedan con `NULL` en esas columnas de la tabla de entrenamiento.

### 10.5 DAG pausado en primera creación

**Síntoma:** Tras el primer arranque, los DAGs aparecían en la UI pero sus disparos no ejecutaban ninguna tarea.

**Causa raíz:** En Airflow, los DAGs nuevos se crean en estado **pausado** por defecto como medida de seguridad: evita que un DAG con errores de programación se ejecute automáticamente nada más importarse.

**Solución:** Despausar los DAGs desde la UI (toggle en la columna izquierda) o desde la CLI:

```bash
airflow dags unpause clock
airflow dags unpause pipeline
```

Una vez despaused, los DAGs permanecen activos entre reinicios del contenedor (el estado se almacena en PostgreSQL).

### 10.6 Dos runs concurrentes bloqueándose mutuamente

**Síntoma:** El DAG `clock` se disparó dos veces seguidas (manualmente durante las pruebas), creando dos runs del DAG `pipeline` que se bloqueaban mutuamente intentando hacer MERGE en las mismas tablas Delta.

**Solución:** `max_active_runs=1` en el DAG `pipeline`. El segundo run del pipeline no se activa hasta que el primero termina.

Adicionalmente, el `TriggerDagRunOperator` con `wait_for_completion=True` en el reloj garantiza que el reloj no dispara un segundo tick mientras el pipeline del primero está en curso.

---

## 11. Guía de operación

### 11.1 Primer arranque

```bash
# 1. Crear el fichero .env con las claves necesarias
cp .env.example .env
# Editar .env para poner AIRFLOW_FERNET_KEY y AIRFLOW_JWT_SECRET únicos

# 2. Levantar dependencias primero
docker compose up minio postgres -d

# 3. Crear los buckets en MinIO (one-shot, puede tardar ~30s en que minio esté listo)
docker compose up minio-init

# 4. Inicializar Airflow (migra la BD y crea el usuario)
docker compose up airflow-init

# 5. Levantar el stack completo
docker compose up -d

# 6. Acceder a la UI
#    http://localhost:8085   usuario: airflow   contraseña: airflow

# 7. Despausar los DAGs desde la UI o:
docker compose exec airflow-scheduler airflow dags unpause clock
docker compose exec airflow-scheduler airflow dags unpause pipeline
```

### 11.2 Lanzar un tick manualmente

Desde la UI de Airflow (http://localhost:8085):
1. Ir al DAG `clock`.
2. Clic en el botón "▶" (Trigger DAG).
3. Confirmar sin parámetros adicionales.

Desde la CLI:
```bash
docker compose exec airflow-scheduler airflow dags trigger clock
```

El tick avanza `sim_day` un día, publica los datos del mes correspondiente (si no están ya en MinIO) y ejecuta el pipeline completo.

### 11.3 Activar avance automático

Editar `dags/clock.py`:

```python
@dag(
    dag_id="clock",
    schedule="*/5 * * * *",   # ← cambiar de None a un cron
    ...
)
```

Con este schedule, el reloj avanza un día cada 5 minutos reales. La cohorte completa (547 días) se simularía en ≈ 45 horas.

Alternativamente, para acelerar la simulación sin modificar el DAG, se puede usar el script de publicación en modo `--ticks all` para poblar MinIO con todos los meses de una vez, y luego ejecutar el pipeline manualmente las veces necesarias hasta que el watermark alcance el último mes.

### 11.4 Resetear la simulación

Para volver al estado inicial (sim_day = 2023-12-31, sin datos en Bronze/Silver/Gold):

```bash
# 1. Parar el stack
docker compose down

# 2. Borrar solo los volúmenes de datos (no el de postgres, para conservar la config Airflow)
#    O borrar todo para un reset completo:
docker compose down -v

# 3. Borrar los objetos de MinIO manualmente (si se preservó el volumen):
#    mc rm --recursive --force local/landing
#    mc rm --recursive --force local/bronze
#    mc rm --recursive --force local/silver
#    mc rm --recursive --force local/gold

# 4. Desde la UI de Airflow: Admin → Variables → borrar sim_day
#    O desde CLI:
docker compose exec airflow-scheduler airflow variables delete sim_day

# 5. Volver a levantar
docker compose up -d
```

---

## 12. Decisiones de diseño relevantes

### confirm_clock separado de advance_clock

El avance del reloj se divide en dos tareas (`advance_clock` calcula, `confirm_clock` escribe) en lugar de calcular y escribir en una sola tarea. Esto garantiza que el reloj no avanza si la publicación de datos falla: si `publish_sources` falla, `confirm_clock` no se ejecuta (por la dependencia `pub >> conf`), y el siguiente tick reintentará publicar el mismo día.

Si se hiciera en una sola tarea que publicara y luego actualizara `sim_day`, un fallo a mitad de la publicación dejaría `sim_day` en el valor antiguo (correcto), pero una publicación parcialmente exitosa con fallo posterior también dejaría el reloj sin avanzar. La separación hace explícito el punto de no-retorno.

### BashOperator en lugar de SparkSubmitOperator

`SparkSubmitOperator` es semánticamente más apropiado que `BashOperator` para este caso de uso: encapsula la lógica de conexión al cluster, el classpath, y los parámetros de Spark. Sin embargo, en Airflow 3 con LocalExecutor, `SparkSubmitOperator` no funciona porque el provider `apache-airflow-providers-apache-spark` no implementa la interfaz de la Execution API del nuevo Task SDK (las tareas quedan en `queued` indefinidamente).

`BashOperator` sí implementa correctamente el nuevo Task SDK. La pérdida es cosmética: en lugar de ver los parámetros Spark en la UI de Airflow, se ve el comando completo de bash. La funcionalidad es idéntica.

### schedule=None en ambos DAGs

Ninguno de los dos DAGs tiene schedule propio. El pipeline se dispara exclusivamente desde el reloj (o manualmente para depuración). El reloj se dispara manualmente o, si se desea automatización, añadiendo un cron al DAG `clock`.

Esta elección evita que el pipeline se ejecute por su cuenta en paralelo con el reloj, lo que podría producir runs con datos a medias (el pipeline arranca antes de que el reloj publique los datos del día).

### Datos sintéticos granulados por mes

Cada fichero de `synthetic_data/` contiene los registros de un mes completo. Granularidades más finas (por día, por semana) harían el directorio más grande y los scripts de publicación más complejos. Granularidades más gruesas (por trimestre, por año) harían imposible la simulación día a día: no se podría publicar "los datos del día 15 de enero" si el fichero contiene todo enero junto (habría que filtrar el CSV, lo que sería un paso de transformación que se salta la filosofía de landing).

La granularidad mensual es un compromiso: el reloj avanza día a día (para mantener la semántica temporal precisa de las etiquetas), pero la publicación efectiva de datos nuevos ocurre solo el primer día de cada mes (días 2-30/31 del mismo mes son idempotentes y no suben nada nuevo).

### LEFT JOIN con `_read_or_empty` en Gold

En lugar de condicionar la construcción del training table a la existencia de todas las fuentes, Gold intenta leer cada fuente opcional y, si no existe, usa un DataFrame vacío. El LEFT JOIN posterior añade columnas `NULL` para todos los pacientes cuando la fuente está vacía.

Esta decisión hace que la tabla `gold/training` siempre exista y siempre tenga el mismo esquema columnar, independientemente del mes simulado. Las columnas de SPPB y lifestyle simplemente son `NULL` hasta que esas fuentes tienen datos. El modelo ML downstream puede manejar `NULL` con imputers o simplemente ignorar esas columnas hasta que estén disponibles.

La alternativa (no escribir Gold si alguna fuente falta) haría imposible usar la tabla de entrenamiento en los primeros 5 meses de la cohorte, que representan casi el 30% de los datos.

### Anti-leakage mediante as-of join en Gold

Las etiquetas de fragilidad no se unen por fecha de snapshot sino por `label_available_date`: la fecha en que el diagnóstico estuvo disponible en el sistema, que puede ser días o semanas posterior a la toma de la muestra. El `_asof_join_latest` con la condición `label_available_date <= snapshot_date` garantiza que solo se joinen etiquetas que ya habrían sido conocidas en la fecha del snapshot clínico.

En enero de 2024, las etiquetas confirmadas el 2024-01-08 no se joinen con el snapshot del 2024-01-01 (`label_available_date > snapshot_date`). El campo `frailty_label` queda `NULL` correctamente para esos pacientes.

Esta es la propiedad más importante del pipeline para la validez del modelo ML: sin anti-leakage, el modelo podría entrenarse con información del futuro y producir métricas de validación irrealistas.
