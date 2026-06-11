# Arquitectura y funcionamiento del pipeline Medallion — TFM Frailty Prediction

## Índice

1. [Visión general](#1-visión-general)
2. [Infraestructura Docker](#2-infraestructura-docker)
3. [Estructura del paquete Python](#3-estructura-del-paquete-python)
4. [Módulo de configuración — `config.py`](#4-módulo-de-configuración--configpy)
5. [Fábrica de SparkSession — `spark_session.py`](#5-fábrica-de-sparksession--spark_sessionpy)
6. [Reglas de calidad — `pipeline/rules/`](#6-reglas-de-calidad--pipelinerules)
7. [Capa Bronze — ingesta incremental](#7-capa-bronze--ingesta-incremental)
   - 7.1 [Watermark — `bronze/watermark.py`](#71-watermark--bronzewatermarkpy)
   - 7.2 [Ingesta de ficheros — `bronze/ingest_clinical.py`](#72-ingesta-de-ficheros--bronzeingest_clinicalpy)
   - 7.3 [Ingesta de JSONL desde landing — `bronze/ingest_gait.py`](#73-ingesta-de-jsonl-desde-landing--bronzeingest_gaitpy)
8. [Capa Silver — validación y cuarentena](#8-capa-silver--validación-y-cuarentena)
   - 8.1 [Motor de cuarentena — `silver/quarantine.py`](#81-motor-de-cuarentena--silverquarantinepy)
   - 8.2 [Transformaciones por fuente](#82-transformaciones-por-fuente)
9. [Capa Gold — agregación y tabla ML](#9-capa-gold--agregación-y-tabla-ml)
   - 9.1 [Features de marcha — `gold/gait_features.py`](#91-features-de-marcha--goldgait_featurespy)
   - 9.2 [Tabla de entrenamiento — `gold/training_table.py`](#92-tabla-de-entrenamiento--goldtraining_tablepy)
10. [Scripts de publicación — `scripts/publish_*.py`](#10-scripts-de-publicación--scriptspublish_py)
11. [Dispatcher del pipeline — `scripts/run_layer.py`](#11-dispatcher-del-pipeline--scriptsrun_layerpy)
12. [Suite de tests — `tests/pipeline/`](#12-suite-de-tests--testspipeline)
13. [Flujo de datos de extremo a extremo](#13-flujo-de-datos-de-extremo-a-extremo)
14. [Decisiones de diseño relevantes](#14-decisiones-de-diseño-relevantes)

---

## 1. Visión general

El pipeline implementa la **arquitectura Medallion** (Bronze → Silver → Gold) sobre un stack completamente local y containerizado. El objetivo final es producir una tabla de entrenamiento ML lista para consumir, que combine datos de cuatro fuentes heterogéneas de 10.000 pacientes mayores de 65 años en seguimiento de fragilidad.

```
Fuentes                Pipeline                           Destino final
────────               ─────────────────────────────────  ──────────────
Source A  (CSV)    ──► BRONZE/clinical                    ┐
Source B1 (JSONL)  ──► BRONZE/sppb                        │
Source B2 (JSONL)  ──► BRONZE/lifestyle                   ├─► GOLD/training
Source C  (JSONL)  ──► BRONZE/gait ──► GOLD/gait_features │    (145K filas × 52 cols)
Labels    (CSV)    ──► BRONZE/labels ─────────────────────┘
                        │
                        └─► SILVER (typed + validated + quarantine)
```

**Stack tecnológico:**

| Componente | Versión | Rol |
|---|---|---|
| Apache Spark | 4.0.1 (Scala 2.13) | Motor de procesamiento distribuido |
| Delta Lake | 4.0.0 | Formato transaccional sobre S3A; ACID, time-travel |
| MinIO | RELEASE.2025-04-22 | Almacén de objetos S3-compatible (equivalente a S3 en AWS) |
| PySpark | 4.0.1 | API Python de Spark |
| Apache Airflow | 3.0.1 | Orquestación de DAGs (reloj de simulación + pipeline) |
| MLflow | latest | Tracking de experimentos ML (fase siguiente) |

---

## 2. Infraestructura Docker

El fichero `docker-compose.yml` levanta los siguientes servicios:

### Servicios de datos

**`minio`** — Objeto store. Expone la API S3 en el puerto 9000 y la UI en el 9001. Los buckets se crean automáticamente mediante el servicio `minio-init`:

```
landing/   ← ficheros crudos publicados por los scripts Python locales
bronze/    ← tablas Delta Lake de ingesta
silver/    ← tablas Delta Lake limpias y cuarentenas
gold/      ← tablas Delta Lake agregadas y ML-ready
```

**`postgres`** — Base de datos de metadatos de Airflow (DAGs, runs, conexiones, variables) y MLflow.

### Servicios de orquestación

**`airflow-init`** — Contenedor one-shot que inicializa el entorno antes de que arranquen el scheduler y el apiserver:
1. `airflow db migrate` — aplica las migraciones SQL al PostgreSQL.
2. `python3 /opt/airflow/init_auth.py` — escribe el fichero de contraseñas de `SimpleAuthManager`.

**`airflow-scheduler`** — Núcleo de Airflow. Parsea los DAGs del directorio `dags/`, programa las ejecuciones y lanza las tareas directamente como subprocesos (`LocalExecutor`). Con `STANDALONE_DAG_PROCESSOR=False`, el dag-processor corre dentro del mismo proceso, eliminando un contenedor extra.

**`airflow-apiserver`** — Interfaz pública de Airflow. Expone la UI web en `http://localhost:8085` y la API REST v2. También sirve la Execution API (`/execution/`) que el LocalExecutor usa para comunicar el estado de cada tarea al scheduler. Esta separación permite que la UI permanezca operativa aunque el scheduler esté ejecutando un job intensivo de Spark.

### Servicios de cómputo

**`spark-master`** — Nodo maestro de Spark. Expone la UI en el 8080. Monta los volúmenes:
- `./spark-apps → /opt/spark-apps` : código del pipeline
- `./synthetic_data → /opt/synthetic_data` : datos sintéticos
- `./tests → /opt/tests` : suite de tests

Las credenciales de MinIO se inyectan como variables de entorno con prefijo `SPARK_CONF_spark.hadoop.fs.s3a.*` para que el proceso maestro las lea al arrancar.

**`spark-worker-1/2`** — Dos workers que se registran en el maestro y ejecutan los tasks distribuidos.

### Cómo se conectan los contenedores a MinIO

Todos los JARs necesarios para el acceso S3A y Delta Lake ya están pre-instalados en `/opt/spark/jars/` de la imagen Docker. No es necesario pasar `spark.jars.packages` en tiempo de ejecución. Esto evita descargas externas y garantiza reproducibilidad.

---

## 3. Estructura del paquete Python

```
spark-apps/
├── pipeline/
│   ├── __init__.py            # Re-exporta constantes de config
│   ├── config.py              # Rutas S3A, claves de dedup y columnas de fecha
│   ├── spark_session.py       # Fábrica de SparkSession
│   ├── rules/
│   │   ├── __init__.py        # get_rules(tag) → {nombre: constraint}
│   │   ├── clinical.py        # 19 reglas para Source A
│   │   ├── sppb.py            # 10 reglas para Source B1
│   │   ├── lifestyle.py       # 10 reglas para Source B2
│   │   ├── gait.py            # 13 reglas para Source C
│   │   └── labels.py          # 5 reglas para flujo Labels (incl. coherencia temporal)
│   ├── bronze/
│   │   ├── watermark.py       # read_watermark / write_watermark
│   │   ├── ingest_clinical.py # _ingest_batch genérico + run_* por fuente de fichero
│   │   └── ingest_gait.py     # Thin wrapper: llama a _ingest_batch para gait JSONL
│   ├── silver/
│   │   ├── quarantine.py      # apply_rules_and_split → (valid, quarantine)
│   │   ├── transform_clinical.py
│   │   ├── transform_sppb.py
│   │   ├── transform_lifestyle.py
│   │   ├── transform_gait.py
│   │   └── transform_labels.py
│   └── gold/
│       ├── gait_features.py   # Agrega por sesión: 13 métricas + 3 derivadas
│       └── training_table.py  # As-of join: clinical ← gait ← sppb ← lifestyle ← labels
└── scripts/
    ├── publish_clinical.py    # Sube CSV a MinIO landing (idempotente)
    ├── publish_sppb.py        # Sube JSONL a MinIO landing (idempotente)
    ├── publish_lifestyle.py   # Sube JSONL a MinIO landing (idempotente)
    ├── publish_gait.py        # Sube JSONL a MinIO landing (idempotente)
    ├── publish_labels.py      # Sube CSV a MinIO landing (idempotente)
    └── run_layer.py           # CLI: bronze | silver | gold

tests/pipeline/
    ├── conftest.py            # Fixture SparkSession de sesión
    ├── test_bronze.py         # 16 tests: idempotencia, auditoría, watermark
    ├── test_silver.py         # 21 tests: tipos, reglas, cuarentena, coherencia temporal
    └── test_gold.py           # 9 tests: sesiones, columnas, anti-leakage
```

---

## 4. Módulo de configuración — `config.py`

**Fichero:** `spark-apps/pipeline/config.py`

Este módulo centraliza todas las constantes del pipeline. No contiene lógica: sólo lee variables de entorno y construye rutas S3A.

### Lectura de nombres de bucket desde entorno

```python
_LANDING = os.getenv("MINIO_BUCKET_LANDING", "landing")
_BRONZE  = os.getenv("MINIO_BUCKET_BRONZE",  "bronze")
_SILVER  = os.getenv("MINIO_BUCKET_SILVER",  "silver")
_GOLD    = os.getenv("MINIO_BUCKET_GOLD",     "gold")
```

Los valores por defecto coinciden con los buckets creados en MinIO. Cambiando las variables de entorno, el mismo código funciona en AWS S3 sin ninguna modificación.

### Función auxiliar `_s3`

```python
def _s3(bucket: str, *parts: str) -> str:
    path = "/".join(parts)
    return f"s3a://{bucket}/{path}" if path else f"s3a://{bucket}"
```

Construye una URI S3A. El protocolo `s3a://` es la implementación de Hadoop optimizada para acceso a objetos (multithreading, multipart upload), diferente de `s3://` (legacy).

### Clases-namespace de rutas

Las cuatro capas se representan como clases con atributos de clase (no instancias):

```python
class BRONZE:
    CLINICAL   = _s3(_BRONZE, "clinical")     # s3a://bronze/clinical
    SPPB       = _s3(_BRONZE, "sppb")
    LIFESTYLE  = _s3(_BRONZE, "lifestyle")
    GAIT       = _s3(_BRONZE, "gait")
    WATERMARKS = _s3(_BRONZE, "_control", "watermarks")  # s3a://bronze/_control/watermarks
```

Este patrón evita colisiones de nombres (`BRONZE_CLINICAL` vs `SILVER_CLINICAL`) y permite imports limpios: `from pipeline.config import BRONZE; BRONZE.CLINICAL`.

### `DEDUP_KEYS` y `DATE_COLS`

```python
DEDUP_KEYS: dict[str, list[str]] = {
    "clinical":  ["patient_id", "snapshot_date"],  # clave natural compuesta
    "sppb":      ["response_id"],                  # UUID por encuesta
    "lifestyle": ["response_id"],
    "gait":      ["event_id"],                     # UUID por zancada
    "labels":    ["patient_id", "snapshot_date"],  # un label confirmado por snapshot
}
DATE_COLS: dict[str, str] = {
    "clinical":  "snapshot_date",
    "sppb":      "survey_date",
    "lifestyle": "survey_date",
    "gait":      "session_timestamp",
    "labels":    "label_available_date",  # cuándo llegó el diagnóstico confirmado
}
```

`DEDUP_KEYS` se usa para construir la condición de MERGE en Bronze. `DATE_COLS` determina por qué columna se filtra el watermark y qué columna se usa para extraer `year`/`month` de particionado.

Para labels, el particionado es por `label_available_date` (no por `snapshot_date`) porque es la columna que gobierna cuándo el registro "llega" al sistema — es la que determina qué etiquetas pueden usarse sin introducir data leakage.

---

## 5. Fábrica de SparkSession — `spark_session.py`

**Fichero:** `spark-apps/pipeline/spark_session.py`

Toda la configuración de Spark está encapsulada en una única función `get_spark()`. Cada módulo que necesita Spark llama a esta función y pasa la sesión obtenida como parámetro a las funciones del pipeline.

### Resolución de credenciales

Hay dos contextos de ejecución posibles y cada uno expone las credenciales de forma diferente:

```python
endpoint   = (os.getenv("MINIO_ENDPOINT")
              or os.getenv("SPARK_CONF_spark.hadoop.fs.s3a.endpoint", "http://minio:9000"))
access_key = (os.getenv("AWS_ACCESS_KEY_ID")
              or os.getenv("SPARK_CONF_spark.hadoop.fs.s3a.access.key", ""))
secret_key = (os.getenv("AWS_SECRET_ACCESS_KEY")
              or os.getenv("SPARK_CONF_spark.hadoop.fs.s3a.secret.key", ""))
```

- **Contenedor `spark-master`**: el proceso JVM de Spark lee las variables `SPARK_CONF_*` al arrancar y las aplica a su propia configuración interna. Sin embargo, cuando Python lanza una nueva `SparkSession` (que es un proceso separado), ésta **no hereda** esa configuración automáticamente — hay que leerlas explícitamente desde el entorno.
- **Contenedores de Airflow**: exponen `AWS_ACCESS_KEY_ID` y `AWS_SECRET_ACCESS_KEY` de forma estándar.

El operador `or` garantiza que siempre se usan las credenciales correctas sin importar en qué contenedor se ejecuta el código.

### Configuración de Delta Lake

```python
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
```

Estas dos líneas son obligatorias para habilitar Delta Lake. Sin ellas, leer o escribir con `.format("delta")` lanzaría un error. `DeltaSparkSessionExtension` añade las funciones SQL de Delta (`DESCRIBE HISTORY`, `MERGE INTO`, etc.) y `DeltaCatalog` reemplaza el catálogo por defecto de Spark para que reconozca tablas Delta de forma nativa.

### Configuración de S3A

```python
.config("spark.hadoop.fs.s3a.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem")
.config("spark.hadoop.fs.s3a.path.style.access", "true")
.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
.config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
```

- `path.style.access = true`: MinIO no soporta el estilo virtual-hosted (`bucket.minio.local`), sólo path-style (`minio:9000/bucket`).
- `ssl.enabled = false`: el entorno local no tiene TLS configurado.
- `SimpleAWSCredentialsProvider`: usa directamente access_key/secret_key, sin intentar resolver credenciales desde EC2 instance profiles o IAM roles (lo que causaría timeouts en un entorno local).

### Memoria configurable

```python
mem = driver_memory or os.getenv("SPARK_DRIVER_MEMORY", "2g")
```

El parámetro `driver_memory` permite aumentar la memoria del driver para datasets grandes (como los 7.4M eventos de gait) sin modificar el código. Se puede pasar como argumento o como variable de entorno: `SPARK_DRIVER_MEMORY=4g`.

---

## 6. Reglas de calidad — `pipeline/rules/`

### Estructura de una regla

Cada regla es un diccionario Python con tres campos:

```python
{"name": "valid_age", "constraint": "age >= 65 AND age <= 95", "tag": "clinical"}
```

- `name`: identificador único. Si una fila falla esta regla, el nombre aparece en el array `failed_rules` de la cuarentena.
- `constraint`: expresión SQL válida que se evalúa sobre una columna de Spark (`F.expr(constraint)`). Debe devolver `true` si la fila es **válida**.
- `tag`: identificador de la fuente. Permite que el índice `__init__.py` filtre las reglas por fuente.

### `rules/__init__.py` — función `get_rules`

```python
def get_rules(tag: str) -> dict[str, str]:
    all_rules = (get_clinical_rules() + get_sppb_rules()
                 + get_lifestyle_rules() + get_gait_rules())
    return {r["name"]: r["constraint"] for r in all_rules if r["tag"] == tag}
```

Carga todas las reglas de los cuatro módulos, filtra por `tag` y devuelve un diccionario `{nombre → constraint}`. Este diccionario es la entrada directa del motor de cuarentena en Silver.

### Reglas de Source A — `rules/clinical.py` (20 reglas)

Las reglas cubren cinco categorías:

| Categoría | Variables | Lógica de validación |
|---|---|---|
| Identidad | `patient_id`, `snapshot_date` | `IS NOT NULL` |
| Demografía | `age`, `sex`, `height_cm`, `weight_kg`, `bmi` | Rangos derivados del generador sintético (ej: `age >= 65 AND age <= 95`) |
| Clínica | `heart_rate_bpm`, `tug_time_s`, `grip_strength_kg`, `mmse`, `gds`, `frailty_index_fi` | Rangos fisiológicos razonables |
| Criterios Fried | `fried_weight_loss`, `fried_weakness`, etc. | `IN (0, 1)` (flags binarios) |
| Consistencia | `bmi` | `ABS(bmi - weight_kg / POW(height_cm / 100.0, 2)) < 0.01` |

La regla de consistencia del BMI es un ejemplo de validación **cruzada entre columnas**: verifica que el campo `bmi` sea coherente con `weight_kg` y `height_cm`. El margen de 0.01 permite el error de redondeo de dos decimales.

### Reglas de Source B1 — `rules/sppb.py` (10 reglas)

Incluye la invariante aritmética del test SPPB:
```python
{"name": "sppb_total_consistent",
 "constraint": "sppb_total = sppb_balance + sppb_gait_speed + sppb_chair_stand"}
```
Esta regla verifica que el campo total sea la suma exacta de sus tres subcomponentes. Cualquier inconsistencia (corrupted record, truncamiento, etc.) la manda a cuarentena.

### Reglas de Source C — `rules/gait.py` (13 reglas)

La regla más importante es la consistencia temporal de la zancada:
```python
{"name": "stride_time_consistent",
 "constraint": "ABS(stance_time_s + swing_time_s - stride_duration_s) < 0.001"}
```
Biomecánicamente, una zancada completa = fase de apoyo (stance) + fase de vuelo (swing). Si la suma no coincide con la duración total (con margen de 1ms por redondeo), el evento se considera inválido. En los datos reales, 3 de 7.4M eventos fallaron esta regla.

---

## 7. Capa Bronze — ingesta incremental

Bronze es la primera capa del lakehouse. Almacena los datos **tal como llegaron** de la fuente, añadiendo únicamente columnas de auditoría y particionado. El objetivo es tener un registro permanente e inmutable del dato crudo, con garantía de no duplicados.

### 7.1 Watermark — `bronze/watermark.py`

El watermark es el mecanismo que hace la ingesta **incremental**: permite que cada ejecución de Bronze sólo procese datos nuevos, sin releer todo lo anterior.

#### Tabla Delta de watermarks

Los watermarks se almacenan en una tabla Delta en `s3a://bronze/_control/watermarks/` con el esquema:

```
source STRING | last_processed STRING | updated_at TIMESTAMP
─────────────────────────────────────────────────────────────
clinical      | 2025-06-01            | 2025-06-28 17:57:22
sppb          | 2025-06-28T17:59:00Z  | 2025-06-28 17:59:10
gait          | 2025-06-18T18:15:00Z  | 2025-06-28 18:05:33
labels        | 2025-07-01            | 2025-06-28 17:59:55
```

El campo `last_processed` es `STRING` intencionalmente: permite almacenar tanto fechas ISO8601 (formato `YYYY-MM-DD` para fuentes CSV con fechas puras como `snapshot_date`) como timestamps completos ISO8601 (formato `YYYY-MM-DDTHH:MM:SSZ` para fuentes JSONL como gait con campo `session_timestamp`). Todas las fuentes usan marcas temporales comparables lexicográficamente en su formato ISO, lo que hace el filtro `date_col > wm` correcto sin necesidad de casting.

#### `read_watermark(spark, source) → str | None`

```python
def read_watermark(spark, source):
    if not DeltaTable.isDeltaTable(spark, BRONZE.WATERMARKS):
        return None   # Primera ejecución: no hay tabla aún
    df = spark.read.format("delta").load(BRONZE.WATERMARKS)
    row = df.filter(F.col("source") == source).select("last_processed").first()
    return row["last_processed"] if row else None
```

`DeltaTable.isDeltaTable()` comprueba si la ruta existe y contiene un directorio `_delta_log/`. Si la tabla no existe (primera ejecución), devuelve `None` directamente sin lanzar excepción.

#### `write_watermark(spark, source, value)`

```python
def write_watermark(spark, source, value):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    new_row = spark.createDataFrame([(source, value, now)], schema="...")
    if DeltaTable.isDeltaTable(spark, BRONZE.WATERMARKS):
        DeltaTable.forPath(spark, BRONZE.WATERMARKS).alias("t")
            .merge(new_row.alias("s"), "t.source = s.source")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        new_row.write.format("delta").save(BRONZE.WATERMARKS)
```

El MERGE garantiza que si ya existe una fila para esa fuente, se actualiza en lugar de insertarse una nueva. Así la tabla siempre tiene exactamente una fila por fuente.

**Invariante crítica**: `write_watermark` sólo se llama **después** de que el MERGE de datos complete con éxito. Si el job falla antes de escribir el watermark, la próxima ejecución reintentará desde el mismo punto de partida — sin pérdida de datos ni duplicados (gracias al MERGE).

### 7.2 Ingesta de ficheros — `bronze/ingest_clinical.py`

Este módulo procesa las tres fuentes de fichero (CSV para clinical, JSON para sppb y lifestyle) mediante la función genérica `_ingest_batch`.

#### Flujo de `_ingest_batch`

```
1. Leer watermark actual
2. Leer todos los ficheros del path landing (recursiveFileLookup=true)
3. Filtrar filas con date_col > watermark  (si hay watermark)
4. Añadir columnas de auditoría: ingestion_timestamp, source_file
5. Añadir columnas de particionado: year, month (desde date_col)
6. Cachear el DataFrame
7. Contar filas (materializa el plan)
8. MERGE or CREATE en la tabla Bronze
9. Calcular max(date_col) y escribir watermark
10. Deshacer caché
```

#### Lectura con `recursiveFileLookup`

```python
reader = spark.read.option("recursiveFileLookup", "true")
df = reader.option("header", "true").option("inferSchema", "true").csv(landing_path)
```

`recursiveFileLookup=true` hace que Spark explore todos los subdirectorios bajo `landing_path`. Esto es necesario porque los ficheros clínicos se suben con estructura `clinical/2024/01/2024-01.csv`, no directamente en la raíz del bucket. Sin esta opción, Spark sólo leería los ficheros del primer nivel.

`inferSchema=true` hace que Spark escanee una muestra del CSV para detectar tipos automáticamente. Los CSV tienen columnas numéricas (age, bmi, etc.) que sin esta opción llegarían como STRING.

#### Filtro por watermark

```python
if wm:
    df = df.filter(F.col(date_col) > wm)
```

Este filtro aplica **push-down**: cuando el formato de fichero admite predicados (Parquet, Delta), Spark puede leer sólo los ficheros relevantes. Para CSV no hay push-down, pero el filtro sigue siendo correcto: se descartan las filas ya procesadas antes de escribir en Bronze.

#### Columnas de auditoría

```python
df = (
    df
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
)
```

- `current_timestamp()`: marca el momento exacto en que Spark procesó el dato. Permite reconstruir qué datos estaban disponibles en cada momento.
- `input_file_name()`: devuelve la ruta S3A del fichero origen. Permite trazabilidad hacia atrás: dado un registro en Bronze, se puede saber de qué fichero vino.

#### Columnas de particionado

```python
df = (
    df
    .withColumn("_d", F.to_date(F.col(date_col)))
    .withColumn("year",  F.year(F.col("_d")))
    .withColumn("month", F.month(F.col("_d")))
    .drop("_d")
)
```

Las tablas Delta se particionan por `year`/`month`. Esto permite que consultas que filtren por fecha (la mayoría en un contexto temporal) lean sólo las particiones relevantes, sin escanear toda la tabla.

#### MERGE idempotente

```python
def _merge_or_create(spark, df, path, merge_cond, partition_cols=None):
    if DeltaTable.isDeltaTable(spark, path):
        DeltaTable.forPath(spark, path).alias("t")
            .merge(df.alias("s"), merge_cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
    else:
        df.write.format("delta").mode("overwrite").partitionBy(*partition_cols).save(path)
```

La primera vez que se ejecuta, la tabla no existe: se crea con `write`. Las veces siguientes, se hace MERGE con la condición de deduplicación (ej: `t.patient_id = s.patient_id AND t.snapshot_date = s.snapshot_date` para clinical). Si la fila ya existe, se actualiza (por si el dato fuente fue corregido); si no existe, se inserta. Esto garantiza **idempotencia**: ejecutar Bronze dos veces produce el mismo resultado que ejecutarlo una vez.

### 7.3 Ingesta de JSONL desde landing — `bronze/ingest_gait.py`

La Source C (eventos de marcha) sigue exactamente el mismo patrón de ingesta incremental por ficheros que el resto de fuentes. `ingest_gait.py` es un thin wrapper sobre `_ingest_batch`:

```python
from pipeline.config import BRONZE, LANDING
from pipeline.bronze.ingest_clinical import _ingest_batch

def run(spark):
    _ingest_batch(spark, "gait", LANDING.GAIT, BRONZE.GAIT, "json")
```

Los ficheros JSONL de eventos de zancada se publican en `landing/gait/YYYY/MM/YYYY-MM.jsonl`. Spark los lee con `spark.read.json(landing_path)` (inferencia de schema sobre JSONL), aplica el filtro de watermark sobre `session_timestamp` y hace el MERGE en Bronze con clave `event_id`.

#### Trazabilidad unificada

Con este patrón, `source_file` de cada evento de zancada contiene la ruta S3A del fichero JSONL del que provino (ej: `s3a://landing/gait/2025/06/2025-06.jsonl`), coherente con el resto de fuentes. Dado un evento en Bronze, se puede recuperar el fichero exacto de landing que lo contuvo.

#### Por qué se eliminó la vía Kafka

La ingesta anterior usaba `spark.read.format("kafka")` — una lectura batch (no streaming) con offsets como mecanismo de progreso. Tenía tres problemas:

1. **No era streaming real**: el job tomaba una instantánea y terminaba, idéntico en semántica a una lectura de ficheros.
2. **La cadencia no justifica un broker**: la Fuente C modela sesiones periódicas (~2 por paciente y mes), no telemetría de alta frecuencia.
3. **Reloj de progreso independiente**: el avance por offset vivía desacoplado del avance por marca temporal del resto de fuentes, complicando la sincronización en Gold.

La unificación elimina el broker, homogeneiza el mecanismo de watermark (todos por marca temporal ISO) y reduce la infraestructura en un servicio.

---

## 8. Capa Silver — validación y cuarentena

Silver recibe los datos de Bronze, aplica el esquema de tipos correcto, evalúa las reglas de calidad y separa el resultado en dos tablas Delta: los registros válidos y los que fallan alguna regla (cuarentena).

**Diferencia fundamental con Bronze**: Silver usa **overwrite** en lugar de MERGE. Silver es una recomputación total desde Bronze cada vez que se ejecuta. Esto es correcto porque Silver no añade información nueva: es una transformación determinista de Bronze. Si Bronze cambia (nuevo dato), ejecutar Silver de nuevo produce el estado correcto.

### 8.1 Motor de cuarentena — `silver/quarantine.py`

La función `apply_rules_and_split` es el núcleo de la validación. Recibe un DataFrame y el diccionario de reglas, y devuelve dos DataFrames: válidos y cuarentena.

#### Paso 1: Evaluar cada regla como columna booleana

```python
for name, constraint in rules.items():
    df = df.withColumn(f"_rule_{name}", F.expr(constraint))
```

Cada regla genera una columna temporal `_rule_<nombre>` con valor `True` (pasa) o `False` (falla). Por ejemplo, la regla `valid_age` genera la columna `_rule_valid_age` con `True` si `age >= 65 AND age <= 95`.

El motivo de evaluar todas las reglas primero (en lugar de filtrar regla a regla) es poder reportar **cuáles** reglas falló cada registro, no sólo si falló.

#### Paso 2: Determinar si la fila pasa todo

```python
rule_cols = [F.col(f"_rule_{name}") for name in rules]
all_pass = rule_cols[0]
for c in rule_cols[1:]:
    all_pass = all_pass & c
df = df.withColumn("is_quarantined", ~all_pass)
```

Se encadenan todas las columnas booleanas con AND. Si cualquiera es `False`, `all_pass` es `False` e `is_quarantined` es `True`.

#### Paso 3: Construir el array de reglas fallidas

```python
failed_array = F.array(*[
    F.when(~F.col(f"_rule_{name}"), F.lit(name))
    for name in rules
])
df = df.withColumn("failed_rules", F.array_compact(failed_array))
```

Para cada regla, si la fila la falló, incluye el nombre de la regla; si la pasó, devuelve `null`. `array_compact` elimina los nulls, dejando sólo los nombres de las reglas fallidas. Por ejemplo: `["valid_age", "bmi_consistent"]`.

Esto es enormemente útil para diagnóstico: en lugar de saber que "N filas fueron rechazadas", se puede analizar exactamente qué reglas están generando más rechazos, lo que suele indicar problemas en la fuente o errores en las reglas.

#### Paso 4: Separar y limpiar

```python
df_valid = (
    df.filter(~F.col("is_quarantined"))
    .drop("is_quarantined", "failed_rules")  # el válido no necesita saber por qué pasó
)
df_quarantine = (
    df.filter(F.col("is_quarantined"))
    .drop("is_quarantined")   # failed_rules se mantiene en cuarentena
)
```

Las columnas temporales `_rule_*` se eliminaron antes. El DataFrame válido no necesita `failed_rules` (pasó todas). La cuarentena mantiene `failed_rules` para diagnóstico.

### 8.2 Transformaciones por fuente

Los cinco módulos `transform_*.py` siguen el mismo patrón general. Sin embargo, **SPPB y lifestyle** tienen un paso previo de guarda porque estas fuentes no tienen datos en todos los meses de la cohorte (SPPB y lifestyle solo comienzan en junio de 2024):

```
1. [sppb / lifestyle] Intentar leer Bronze; si la tabla no existe aún → return
2. Leer la tabla Delta de Bronze
3. Castear columnas al tipo correcto
4. Obtener reglas con get_rules(tag)
5. apply_rules_and_split → (df_valid, df_quarantine)
6. Escribir ambos DataFrames en Silver con overwrite
7. Imprimir conteos
```

#### Guard `PATH_NOT_FOUND` en `transform_sppb.py` y `transform_lifestyle.py`

Cuando el reloj de simulación está en los primeros meses de la cohorte (enero–mayo de 2024), los scripts de publicación de SPPB y lifestyle imprimen `"sin datos para este mes"` y no suben ningún fichero a MinIO. Bronze intenta procesar esas fuentes y tampoco escribe nada (no hay ficheros nuevos). La tabla Delta `s3a://bronze/sppb` no llega a crearse. Si Silver intentara leer esa ruta incondicionalmente, lanzaría `AnalysisException: PATH_NOT_FOUND`.

La guarda captura esa excepción y permite a Silver terminar con éxito:

```python
# transform_sppb.py (ídem en transform_lifestyle.py)
def run(spark: SparkSession) -> None:
    try:
        df = spark.read.format("delta").load(BRONZE.SPPB)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            print("[sppb] Silver: sin datos en bronze todavía, omitiendo")
            return
        raise
    # ... resto de la transformación
```

Esto permite que el pipeline completo (Bronze → Silver → Gold) sea ejecutable desde el primer tick, sin que haya datos de todas las fuentes.

#### Casteos en `transform_clinical.py`

```python
df = (
    df
    .withColumn("snapshot_date",      F.col("snapshot_date").cast(DateType()))
    .withColumn("updated_at",         F.col("updated_at").cast(TimestampType()))
    .withColumn("fried_weight_loss",  F.col("fried_weight_loss").cast(IntegerType()))
    # ... 5 columnas más
)
```

Bronze almacena los datos con los tipos que Spark infirió del CSV (`inferSchema=true`). Este proceso no es perfecto: puede inferir fechas como STRING, o flags binarios (0/1) como INTEGER o DOUBLE según el contexto. El casting explícito en Silver garantiza que downstream (Gold, ML) recibe tipos predecibles y correctos.

Por ejemplo, `snapshot_date` llega de Bronze como STRING (`"2024-03-15"`) y se convierte a `DateType` para que las comparaciones temporales sean semánticas y eficientes.

#### Casteos en `transform_gait.py`

```python
df = df.withColumn("session_timestamp", F.col("session_timestamp").cast(TimestampType()))
for col in metric_cols:
    df = df.withColumn(col, F.col(col).cast(DoubleType()))
```

Los 8 campos de métricas de marcha se castean a `DoubleType` explícitamente. La inferencia de schema de Spark sobre JSONL puede producir `LongType` para campos con valores enteros en los primeros registros muestreados. El cast explícito garantiza `DoubleType` independientemente de lo que la inferencia haya producido.

#### Casteos en `transform_labels.py`

```python
df = (
    df
    .withColumn("snapshot_date",       F.col("snapshot_date").cast(DateType()))
    .withColumn("label_available_date", F.col("label_available_date").cast(DateType()))
    .withColumn("frailty_label",        F.col("frailty_label").cast(IntegerType()))
)
```

Ambas fechas se castean a `DateType` (no `TimestampType`) porque el generador produce fechas puras sin componente horaria. Esto es importante para la regla de coherencia temporal (`label_available_date > snapshot_date`) y para el as-of join en Gold, donde ambas columnas deben ser del mismo tipo para la comparación `<=`.

La regla `temporal_coherence` de `rules/labels.py` verifica que `label_available_date > snapshot_date` (el diagnóstico llegó estrictamente después de la medición). El delay sintético es de 7 a 30 días, por lo que en datos bien formados esta regla nunca debería fallar.

#### Escritura con `overwriteSchema`

```python
df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year", "month")
    .save(path)
```

`overwriteSchema=true` permite que Silver reescriba la tabla incluso si el esquema ha cambiado (ej: se añadió un cast nuevo). Sin esta opción, Delta Lake rechaza la escritura si el esquema de los datos no coincide exactamente con el de la tabla existente — una protección útil en producción pero que impide la iteración rápida durante el desarrollo.

---

## 9. Capa Gold — agregación y tabla ML

Gold es la capa de valor. Toma los datos validados de Silver y los transforma en dos productos:
1. **`gold/gait_features`**: métricas biomecánicas por sesión (agrega zancadas individuales).
2. **`gold/training`**: tabla unificada con todas las variables predictoras y la etiqueta objetivo.

### 9.1 Features de marcha — `gold/gait_features.py`

#### Agrupación por sesión

```python
features = (
    df.groupBy(
        "patient_id",
        "session_id",
        F.to_date("session_timestamp").alias("session_date"),
    )
    .agg(...)
)
```

Cada sesión de marcha contiene entre 40 y 80 zancadas individuales (stride events). Este groupBy colapsa todas las zancadas de una sesión en **una única fila** con métricas estadísticas de la sesión. `session_date` se extrae de `session_timestamp` (casteo a date) para poder usarse como clave temporal en el as-of join de Gold training.

#### Métricas calculadas

| Feature | Fórmula | Significado |
|---|---|---|
| `gait_velocity_ms` | `mean(stride_length / stride_duration)` | Velocidad media de marcha (m/s) |
| `stride_length_m` | `mean(stride_length_m)` | Longitud media de zancada (m) |
| `stride_time_s` | `mean(stride_duration_s)` | Duración media de zancada (s) |
| `cadence_strides_min` | `60.0 / mean(stride_duration_s)` | Zancadas por minuto |
| `swing_time_pct` | `mean(swing_time_s / stride_duration_s) * 100` | % de tiempo en vuelo |
| `stance_time_pct` | `100 - swing_time_pct` | % de tiempo en apoyo (derivado) |
| `foot_clearance_m` | `mean(foot_clearance_m)` | Altura media de levantamiento del pie (m) |
| `toe_off_angle_deg` | `mean(toe_off_angle_deg)` | Ángulo medio de despegue del pie (°) |
| `heel_strike_angle_deg` | `mean(heel_strike_angle_deg)` | Ángulo medio de contacto del talón (°) |
| `lateral_excursion_m` | `mean(lateral_excursion_m)` | Desplazamiento lateral medio (m) |
| `stride_time_cv` | `stddev(stride_duration_s) / mean(stride_duration_s)` | Coeficiente de variación temporal |
| `step_speed_ms` | `stride_length_m / stride_time_s` | Velocidad de paso (derivado) |
| `n_strides` | `count(*)` | Número de zancadas analizadas |

El **coeficiente de variación del tiempo de zancada** (`stride_time_cv`) es especialmente relevante para fragilidad: una marcha frágil tiene mayor irregularidad (CV alto), mientras que una marcha robusta es más regular (CV bajo).

### 9.2 Tabla de entrenamiento — `gold/training_table.py`

#### Problema del join temporal (as-of join)

Las cuatro fuentes tienen cadencias diferentes:
- Clinical: snapshot mensual por paciente
- SPPB/Lifestyle: encuesta trimestral
- Gait: sesión por sesión

Para construir la tabla de entrenamiento, hay que asociar a cada snapshot clínico (`patient_id`, `snapshot_date`) los datos más recientes de las otras fuentes que sean **anteriores o iguales** a la fecha del snapshot. Esto es un **as-of join** (también llamado point-in-time join).

Sin este cuidado, se estaría usando información del futuro para predecir el presente — **data leakage**, el error más grave en ML temporal.

#### Implementación de `_asof_join_latest`

```python
def _asof_join_latest(base, lookup, pid_col, base_date, lookup_date, feature_cols):
    # 1. Preparar lookup con nombres prefijados para evitar colisiones
    lk = lookup.select(
        F.col(pid_col).alias("_lk_pid"),
        F.col(lookup_date).alias("_lk_date"),
        *[F.col(c) for c in feature_cols],
    )

    # 2. Left join con doble condición: mismo paciente Y lookup_date <= base_date
    joined = (
        base
        .join(lk,
              (base[pid_col] == lk["_lk_pid"]) &
              (lk["_lk_date"] <= base[base_date]),
              "left")
        .drop("_lk_pid")
    )

    # 3. Window function: ordenar por fecha y quedarse sólo con la más reciente
    w = Window.partitionBy(pid_col, base_date).orderBy(F.desc("_lk_date"))
    return (
        joined
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_lk_date")
    )
```

**Por qué `left` join**: si un paciente no tiene datos de gait en una fecha concreta, debe aparecer igualmente en la tabla de entrenamiento (con las features de gait a `null`). Un inner join eliminaría esos pacientes, reduciendo artificialmente el dataset.

**Por qué `row_number()` y no `max(lookup_date)`**: `max()` sólo puede usarse con un groupBy. Aquí necesitamos no sólo la fecha máxima sino también las columnas de features correspondientes. La Window function con `row_number()` permite seleccionar toda la fila con la fecha más reciente.

**Por qué se renombran las columnas de lookup**: si `lookup` tiene una columna `patient_id` y `base` también, el join producirá ambigüedad. Al renombrar a `_lk_pid` se elimina la colisión. Después del join se hace `.drop("_lk_pid")` para limpiar.

#### Helper `_read_or_empty` para fuentes opcionales

SPPB y lifestyle solo tienen datos a partir de junio de 2024. En meses anteriores, Silver no genera sus tablas (las guarda con `return` sin escribir nada). Si Gold intentara hacer `spark.read.format("delta").load(SILVER.SPPB)` incondicionalmente, obtendría `PATH_NOT_FOUND`.

El helper `_read_or_empty` encapsula esa tolerancia:

```python
def _read_or_empty(spark: SparkSession, path: str, pid_col: str, date_col: str) -> DataFrame:
    """Lee una tabla Delta; si no existe devuelve un DataFrame vacío con esquema mínimo."""
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

Cuando la tabla no existe devuelve un DataFrame **vacío** con el mismo esquema mínimo que necesita `_asof_join_latest` (la clave de paciente y la columna de fecha). El LEFT JOIN con un DataFrame vacío simplemente deja `NULL` en todas las columnas de SPPB/lifestyle para todos los pacientes, sin eliminar ninguna fila de la tabla de entrenamiento.

#### Secuencia de joins

```python
clinical  = spark.read.format("delta").load(SILVER.CLINICAL)
gait      = spark.read.format("delta").load(GOLD.GAIT_FEATURES)
sppb      = _read_or_empty(spark, SILVER.SPPB,      "patient_id", "survey_date")
lifestyle = _read_or_empty(spark, SILVER.LIFESTYLE, "patient_id", "survey_date")
labels    = spark.read.format("delta").load(SILVER.LABELS)

# Base: silver_clinical
training = _asof_join_latest(clinical, gait,
    pid_col="patient_id", base_date="snapshot_date",
    lookup_date="session_date", ...)

# Añadir SPPB (survey_date → date para comparar con snapshot_date)
sppb = sppb.withColumn("survey_date", F.to_date("survey_date"))
training = _asof_join_latest(training, sppb,
    pid_col="patient_id", base_date="snapshot_date",
    lookup_date="survey_date", ...)

# Añadir Lifestyle
lifestyle = lifestyle.withColumn("survey_date", F.to_date("survey_date"))
training = _asof_join_latest(training, lifestyle, ...)
```

Los joins se aplican secuencialmente: clinical ← gait, ← sppb, ← lifestyle, ← labels. La tabla `gold/training` siempre se escribe con el mismo esquema columnar, independientemente del mes simulado. Las columnas de SPPB y lifestyle son `NULL` hasta que esas fuentes tienen datos (a partir del primer tick de junio de 2024). Con la cohorte completa (18 meses), el resultado es 145.147 filas × 52 columnas.

#### Anti-leakage en el join de etiquetas

```python
labels = spark.read.format("delta").load(SILVER.LABELS)
label_cols = [c for c in labels.columns
              if c not in {"patient_id", "snapshot_date"} | _audit]
training = _asof_join_latest(
    training, labels,
    pid_col="patient_id", base_date="snapshot_date",
    lookup_date="label_available_date", feature_cols=label_cols,
)
```

Para el join de etiquetas, `lookup_date="label_available_date"`. La condición del as-of join es `label_available_date <= snapshot_date`. Esto garantiza que sólo se une una etiqueta si ya estaba disponible en la fecha del snapshot clínico: el diagnóstico llegó antes o en la misma fecha. Una etiqueta con `label_available_date > snapshot_date` (confirmada en el futuro respecto al snapshot) **no se une**, evitando que el modelo vea el futuro durante el entrenamiento.

El campo `label_available_date` se incluye en `label_cols` y por tanto aparece en la tabla de entrenamiento, lo que permite auditar la condición anti-leakage mediante el test `test_no_label_leakage`.

---

## 10. Scripts de publicación — `scripts/publish_*.py`

Los scripts de publicación simulan la llegada de datos desde las fuentes reales. Publican los datos sintéticos al bucket `landing` de MinIO.

Tienen **dos modos de uso**:

| Modo | Quién lo usa | Descripción |
|---|---|---|
| `--day YYYY-MM-DD` | DAG `clock` de Airflow | Publica el mes que contiene ese día (idempotente) |
| `--ticks N\|all` | Usuario manual | Publica N meses pendientes (o todos) de una vez |

### Modo `--day` (usado por el reloj de simulación)

El DAG `clock` llama a cada script desde dentro del contenedor `airflow-scheduler` con `subprocess.run([sys.executable, script, "--day", next_day])`. El script extrae el año y mes del día simulado, localiza el fichero correspondiente y lo sube si no existe aún:

```python
if args.day:
    d = date.fromisoformat(args.day)
    stem = f"{d.year:04d}-{d.month:02d}"          # "2024-01"
    matches = [f for f in all_files if f.stem == stem]
    if not matches:
        print(f"[{stem}] clinical → sin datos para este mes")
        return
    fpath = matches[0]
    key = f"clinical/{d.year:04d}/{d.month:02d}/{fpath.name}"
    if _exists(client, bucket, key):               # HEAD request → idempotente
        print(f"[{stem}] clinical → ya publicado, omitiendo")
        return
    client.upload_file(str(fpath), bucket, key)
```

Si no existe fichero para ese mes (fuentes B1/B2 en ticks anteriores a junio de 2024), imprime `"sin datos para este mes"` y termina **sin error**. Esto permite que el DAG llame a los cinco scripts en todos los ticks sin fallar cuando hay fuentes sin datos.

### Modo `--ticks` (uso manual)

```bash
python publish_clinical.py --ticks 1      # sube el primer mes pendiente
python publish_clinical.py --ticks all    # sube todos los meses pendientes
python publish_clinical.py --ticks all --delay 5  # con pausa entre meses
```

Este modo escanea todos los ficheros del directorio fuente, filtra los que aún no están en MinIO (HEAD request), y publica los primeros `N` o todos.

### Idempotencia — `_exists` con HEAD request

```python
def _exists(client, bucket, key):
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False
```

La petición HEAD es O(1): MinIO responde 200 si el objeto existe, 404 si no. Es mucho más eficiente que listar el bucket completo (`list_objects_v2`). Ambos modos (`--day` y `--ticks`) usan esta función antes de cualquier upload, garantizando que el script sea **idempotente** en ambos contextos.

### Fuentes sin cobertura mensual completa

Las fuentes B1 (SPPB) y B2 (lifestyle) solo tienen datos desde junio de 2024. En ticks anteriores, `--day` imprime `"sin datos para este mes"` y retorna sin error. Bronze tampoco escribe nada (sin ficheros nuevos en landing). Silver detecta la tabla inexistente y retorna también. Gold usa `_read_or_empty` y genera columnas `NULL`. El pipeline completo es ejecutable desde el primer tick sin ninguna configuración especial.

### Estructura de rutas en MinIO

| Fuente | Ruta en landing | Primer mes disponible |
|---|---|---|
| clinical | `landing/clinical/YYYY/MM/YYYY-MM.csv` | 2024-01 |
| sppb | `landing/sppb/YYYY/MM/YYYY-MM.jsonl` | 2024-06 |
| lifestyle | `landing/lifestyle/YYYY/MM/YYYY-MM.jsonl` | 2024-06 |
| gait | `landing/gait/YYYY/MM/YYYY-MM.jsonl` | 2024-01 |
| labels | `landing/labels/YYYY/MM/YYYY-MM.csv` | 2024-01 |

---

## 11. Dispatcher del pipeline — `scripts/run_layer.py`

```python
_LAYERS = {
    "bronze": run_bronze,
    "silver": run_silver,
    "gold":   run_gold,
}

def main():
    layer = sys.argv[1]
    spark = get_spark(f"run-{layer}")
    try:
        _LAYERS[layer](spark)
    finally:
        spark.stop()
```

El dispatcher crea una `SparkSession`, despacha a la función correspondiente y garantiza que la sesión se cierra con `finally` aunque el job falle. Es el punto de entrada manual para ejecutar cualquier capa.

Cada función de capa importa sus dependencias de forma tardía (dentro de la función, no en el nivel de módulo):

```python
def run_bronze(spark):
    from pipeline.bronze.ingest_clinical import run as clinical
    from pipeline.bronze.ingest_gait import run as gait
    clinical(spark)
    gait(spark)
```

Esto evita que un error de importación en un módulo (ej: un JAR faltante) impida ejecutar las otras capas.

---

## 12. Suite de tests — `tests/pipeline/`

### `conftest.py` — fixture de SparkSession compartida

```python
@pytest.fixture(scope="session")
def spark():
    session = get_spark("test-pipeline")
    yield session
    session.stop()
```

`scope="session"` crea **una única SparkSession** para todos los tests del run. Arrancar una SparkSession tarda ~15 segundos; con scope de sesión esto ocurre una sola vez. El `yield` hace que la sesión se cierre automáticamente al finalizar la suite.

### `test_bronze.py` — 16 tests

**`TestBronzeClinical`** (5 tests):
- `test_rows_at_least_as_many_as_source`: bronze ≥ landing (datos anteriores al watermark pueden no estar en landing pero sí en bronze).
- `test_no_duplicates`: `count() == distinct_count()` sobre la clave natural compuesta.
- `test_audit_columns_not_null`: ninguna fila con `ingestion_timestamp` o `source_file` a null.
- `test_watermark_updated`: el watermark existe y es una fecha razonable (> 2024-01-01).
- `test_idempotent`: ejecutar Bronze dos veces da el mismo número de filas.

**`TestBronzeGait`** (4 tests):
- `test_no_duplicates`: no hay `event_id` repetidos.
- `test_audit_columns_not_null`: `source_file` contiene la ruta S3A del fichero JSONL.
- `test_watermark_updated`: el watermark de gait es un timestamp ISO (> "2024-01-01"), no un offset.
- `test_idempotent`: mismo número de filas tras segunda ejecución.

**`TestBronzeLabels`** (5 tests):
- `test_no_duplicates`: no hay `(patient_id, snapshot_date)` repetidos.
- `test_audit_columns_not_null`: columnas de auditoría no nulas.
- `test_watermark_updated`: watermark de labels existe y es razonable.
- `test_rows_match_clinical`: `count(bronze_labels) == count(bronze_clinical)` — 1 etiqueta por snapshot.
- `test_idempotent`: segunda ejecución no cambia el conteo.

### `test_silver.py` — 21 tests

**`TestSilverClinical`** (5 tests):
- `test_valid_plus_quarantine_equals_bronze`: `silver + quarantine == bronze` (conservación de datos).
- `test_no_rule_violations_in_silver`: para cada regla, cero filas en silver que la violen.
- `test_types_after_transform`: schema check con `isinstance(field.dataType, DateType)`. Verifica que `frailty_label` **no está** en silver_clinical (migrado al flujo labels).
- `test_quarantine_has_failed_rules_column`: la tabla de cuarentena tiene la columna `failed_rules`.
- `test_quarantine_triggered_by_invalid_data`: inyección de `age=150` → verifica cuarentena con `"valid_age"` en `failed_rules`.

**`TestSilverLabels`** (5 tests):
- `test_valid_plus_quarantine_equals_bronze`: conservación de volumen.
- `test_no_rule_violations_in_silver`: las 5 reglas de labels no tienen violaciones en silver.
- `test_types_after_transform`: `snapshot_date` y `label_available_date` son `DateType`, `frailty_label` es `IntegerType`.
- `test_quarantine_has_failed_rules_column`: cuarentena tiene `failed_rules`.
- `test_temporal_coherence_violation_quarantined`: inyecta una fila con `label_available_date == snapshot_date` (igualdad, no estrictamente posterior) → verifica que la regla `temporal_coherence` la manda a cuarentena.

### `test_gold.py` — 9 tests

**`TestGoldGaitFeatures`** (4 tests):
- `test_one_row_per_session`: `count() == distinct(session_id).count()`.
- `test_stride_time_cv_positive`: CV > 0 en todas las sesiones.
- `test_expected_columns_present`: las 16 columnas esperadas están presentes.
- `test_n_strides_positive`: todas las sesiones tienen al menos una zancada.

**`TestGoldTraining`** (5 tests):
- `test_at_least_one_row_per_patient`: pacientes únicos en training == pacientes en silver_clinical.
- `test_frailty_label_mostly_present`: nulos en `frailty_label` < 10% (sólo el primer snapshot de cada paciente carece de etiqueta confirmada).
- `test_no_label_leakage`: cero filas donde `label_available_date > snapshot_date` — verifica el as-of join anti-leakage.
- `test_column_count`: ≥ 40 columnas (verifica join multimodal completo).
- `test_rows_match_clinical`: filas en training == filas en silver_clinical.

---

## 13. Flujo de datos de extremo a extremo

El pipeline se ejecuta tick a tick, orquestado por el DAG `clock` de Airflow. Las cifras entre paréntesis corresponden a la **cohorte completa** (18 meses, 2024-01 a 2025-06). En cada tick individual solo se procesan los datos del mes simulado.

```
[Airflow — DAG clock]
        │  advance_clock → publish_sources → confirm_clock → trigger_pipeline
        │
        │  publish_sources llama, por cada tick, a los cinco scripts con --day YYYY-MM-DD:
        │
[Datos sintéticos]
        │
        ├─ source_a/clinical_records/*.csv  ──► publish_clinical.py --day ──► MinIO landing/clinical/
        ├─ source_b1/sppb_surveys/*.jsonl   ──► publish_sppb.py     --day ──► MinIO landing/sppb/
        │                                       (sin datos antes de 2024-06, return sin error)
        ├─ source_b2/lifestyle_*.jsonl      ──► publish_lifestyle.py --day ──► MinIO landing/lifestyle/
        │                                       (sin datos antes de 2024-06, return sin error)
        ├─ source_c/gait_events/*.jsonl     ──► publish_gait.py     --day ──► MinIO landing/gait/
        └─ labels/*.csv                     ──► publish_labels.py   --day ──► MinIO landing/labels/

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BRONZE (Spark + Delta)  ─ _ingest_batch genérico para todas las fuentes
        │
        ├─ read_watermark(clinical)  ←── _control/watermarks
        ├─ spark.read.csv(landing/clinical, recursiveFileLookup=true) → filtrar > wm
        ├─ +ingestion_timestamp, +source_file (ruta S3A), +year, +month
        ├─ MERGE INTO bronze/clinical ON (patient_id, snapshot_date)
        ├─ write_watermark(clinical, max_snapshot_date)                (145.567 filas total)
        │
        ├─ [mismo flujo para sppb (29.206) y lifestyle (29.206) cuando hay datos]
        │
        ├─ spark.read.json(landing/gait) → filtrar session_timestamp > wm
        │  MERGE INTO bronze/gait ON (event_id)
        │  write_watermark(gait, max_session_timestamp)              (10.398.604 filas total)
        │
        └─ spark.read.csv(landing/labels) → filtrar label_available_date > wm
           MERGE INTO bronze/labels ON (patient_id, snapshot_date)
           write_watermark(labels, max_label_available_date)          (145.567 filas total)
                │
━━━━━━━━━━━━━━━━┿━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SILVER (Spark + Delta)  ─ recomputa desde Bronze cada vez
        │
        ├─ read bronze/clinical
        ├─ cast: snapshot_date→Date, updated_at→Timestamp, fried_*→Int
        ├─ get_rules("clinical") → 19 reglas
        ├─ apply_rules_and_split → (df_valid, df_quarantine)
        ├─ write OVERWRITE silver/clinical           (145.147 filas total)
        ├─ write OVERWRITE silver/quarantine_clinical (420 filas total)
        │
        ├─ sppb: intenta leer bronze/sppb
        │  → antes de 2024-06: PATH_NOT_FOUND → return (guarda)
        │  → desde 2024-06: mismo flujo que clinical
        │
        ├─ lifestyle: idéntico a sppb
        │
        ├─ read bronze/gait
        │  cast: session_timestamp→Timestamp, métricas→Double
        │  get_rules("gait") → 13 reglas
        │  apply_rules_and_split → (valid, quarantine)
        │  write OVERWRITE silver/gait          (10.398.598 filas total)
        │  write OVERWRITE silver/quarantine_gait (6 filas total)
        │
        └─ read bronze/labels
           cast: snapshot_date→Date, label_available_date→Date, frailty_label→Int
           get_rules("labels") → 5 reglas (incl. temporal_coherence)
           apply_rules_and_split → (valid, quarantine)
           write OVERWRITE silver/labels           (145.567 filas total)
           write OVERWRITE silver/quarantine_labels (0 filas total)
                │
━━━━━━━━━━━━━━━━┿━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GOLD (Spark + Delta)
        │
        ├─ gait_features.py:
        │   groupBy(patient_id, session_id, to_date(session_timestamp))
        │   .agg(mean velocity, mean length, CV, cadence, ...)
        │   +stance_time_pct (derivada), +step_speed_ms (derivada)
        │   write OVERWRITE gold/gait_features   (259.948 sesiones total)
        │
        └─ training_table.py:
            base   = silver/clinical  (145.147 filas total)
            gait   = gold/gait_features
            sppb   = _read_or_empty(silver/sppb)       ← DataFrame vacío si la tabla no existe
            lifestyle = _read_or_empty(silver/lifestyle) ← ídem
            labels = silver/labels
            _asof_join_latest(base, gait,      lookup_date=session_date)
            _asof_join_latest(result, sppb,    lookup_date=to_date(survey_date))
            _asof_join_latest(result, lifestyle, lookup_date=to_date(survey_date))
            _asof_join_latest(result, labels,  lookup_date=label_available_date)
              └─ condición: label_available_date <= snapshot_date  ← ANTI-LEAKAGE
            write OVERWRITE gold/training  (145.147 filas × 52 columnas con cohorte completa)
                │
━━━━━━━━━━━━━━━━┿━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[Fase siguiente: entrenamiento ML sobre gold/training con MLflow]
```

---

## 14. Decisiones de diseño relevantes

### Por qué Bronze usa MERGE y Silver usa OVERWRITE

**Bronze** recibe datos incrementales (ficheros nuevos en landing filtrados por watermark). El MERGE garantiza dos propiedades:
1. **Idempotencia**: ejecutar dos veces produce el mismo resultado.
2. **Actualización**: si la fuente corrige un dato anterior, el MERGE lo propaga (whenMatchedUpdateAll).

**Silver** es una transformación **determinista y total** de Bronze. No hay estado en Silver que no esté en Bronze: es simplemente Bronze con tipos correctos y sin datos inválidos. Por tanto, la forma más simple y correcta es recalcular Silver completamente con overwrite. Usar MERGE en Silver añadiría complejidad innecesaria.

### Por qué el watermark es STRING y no TIMESTAMP

Las fuentes de fichero producen `snapshot_date` como `YYYY-MM-DD` y `session_timestamp` como `YYYY-MM-DDTHH:MM:SSZ`. Usar STRING como tipo de `last_processed` permite almacenar ambos formatos en la misma tabla sin perder información de precisión: las fechas puras (clinical, labels) se almacenan sin componente horaria, y los timestamps completos (gait, sppb, lifestyle) con ella. Ambos formatos ISO8601 son comparables lexicográficamente, por lo que el filtro `date_col > wm` funciona correctamente para todos.

### Por qué se usa `cache()` / `unpersist()` en Bronze

```python
df = df.cache()
n = df.count()
# ... MERGE ...
df.unpersist()
```

Sin caché, Spark evaluaría el plan de ejecución del DataFrame dos veces: una para el `count()` y otra para el MERGE. Con caché, el DataFrame se materializa en memoria la primera vez y la segunda evaluación lee desde ahí. Esto es especialmente relevante para el dataset de gait (~10M filas): sin caché, Spark relanzaría la lectura de los ficheros JSONL desde MinIO dos veces, duplicando el tiempo de ejecución y el tráfico de red.

### Por qué la etiqueta vive en un flujo independiente y no en Source A

En el dominio real, el diagnóstico clínico de fragilidad (`frailty_label`) no se confirma en el mismo momento que la toma de mediciones (`snapshot_date`). El médico evalúa los datos y emite el diagnóstico días o semanas después. Si la etiqueta viviera en el mismo CSV que las mediciones (Source A), el pipeline nunca podría distinguir entre "la etiqueta estaba disponible cuando se hizo el snapshot" y "la etiqueta fue inferida más tarde y retroalimentada al sistema", abriendo la puerta al data leakage.

El flujo independiente `labels` modela explícitamente este delay mediante `label_available_date = snapshot_date + randint(7, 30) días`. El as-of join en Gold con condición `label_available_date <= snapshot_date` garantiza que sólo se unen etiquetas que ya estaban disponibles en la fecha del snapshot, sin importar cuándo se ingirieron en el sistema.

### Por qué `_lk_pid` / `_lk_date` en el as-of join

```python
lk = lookup.select(
    F.col(pid_col).alias("_lk_pid"),
    F.col(lookup_date).alias("_lk_date"),
    ...
)
```

Cuando dos DataFrames con columnas del mismo nombre se unen con un join que no es `using()`, Spark mantiene ambas copias pero la referencia es ambigua. Al renombrar las columnas clave del lookup con prefijo `_lk_`, se elimina la ambigüedad y se puede hacer `.drop("_lk_pid")` de forma segura al final.

### Por qué `overwriteSchema=true` en Silver y Gold

Delta Lake protege el esquema de sus tablas: si intentas escribir con un esquema diferente al registrado, lanza error. Durante el desarrollo, los casteos y transformaciones pueden cambiar el esquema (añadir una columna, cambiar un tipo). `overwriteSchema=true` desactiva esa protección, lo que es aceptable porque Silver y Gold son capas derivadas que se pueden regenerar completamente desde Bronze.

En producción con esquemas estables, se usaría schema evolution controlada (`mergeSchema=true`) en lugar de `overwriteSchema`.

### Por qué Gold usa `_read_or_empty` en lugar de leer Silver directamente

La tabla de entrenamiento requiere datos de cinco fuentes. Dos de ellas (SPPB y lifestyle) no tienen datos durante los primeros cinco meses de la cohorte (enero–mayo 2024). Si Gold intentara leer `silver/sppb` incondicionalmente, obtendría `PATH_NOT_FOUND` y todo el pipeline fallaría — incluyendo la escritura de los datos de clinical, gait y labels que sí están disponibles.

`_read_or_empty` permite que Gold sea ejecutable desde el primer tick. El LEFT JOIN con un DataFrame vacío no elimina ninguna fila de la tabla de entrenamiento: simplemente deja `NULL` en las columnas de SPPB y lifestyle para todos los pacientes, igual que cuando hay datos pero un paciente concreto no tiene encuesta registrada en esa fecha. El modelo ML downstream puede manejar esos `NULL` con imputers.

La alternativa (no escribir Gold si alguna fuente falta) haría inaccesible el 28% de la cohorte (los primeros 5 meses de 18).

### Por qué los scripts de publicación se ejecutan desde Airflow y no desde el host

En la arquitectura original los scripts se ejecutaban manualmente desde el host. Con el reloj de simulación implementado, el DAG `clock` los llama vía `subprocess.run` desde dentro del contenedor `airflow-scheduler`, que tiene acceso a la red Docker (`minio:9000`) y a los datos sintéticos montados en `/opt/synthetic_data`.

Ejecutarlos desde el host requeriría exponer el puerto 9000 de MinIO y que el host tenga las dependencias Python (`boto3`). Ejecutarlos desde el scheduler elimina esa dependencia externa y mantiene la publicación de datos como un paso del DAG, con reintentos automáticos, logs en la UI de Airflow y trazabilidad de fallos.
