# Inconsistencias entre la memoria (ch2–ch4) y el código actual

> Generado el 2026-06-19 tras una revisión exhaustiva del código fuente.
> Motivación: el pipeline sufrió un cambio arquitectónico sustancial que
> invalida varias secciones de la memoria. Este documento recoge, capítulo
> a capítulo, qué ha cambiado y qué debe reescribirse.

---

## Índice

1. [Capítulo 2 — Estado del arte](#cap2)
2. [Capítulo 3 — Dataset y fuentes de datos](#cap3)
3. [Capítulo 4 — Arquitectura del pipeline](#cap4)
4. [Bugs críticos (pipeline roto)](#bugs)

---

## 1. Capítulo 2 — Estado del arte <a name="cap2"></a>

**Impacto: BAJO.** El capítulo es académico (revisión de literatura sobre
fragilidad, MLOps, arquitecturas medallion) y no hace referencia directa a
implementación concreta. No se identifican inconsistencias técnicas con el
código. Sin embargo, hay una advertencia menor:

- Si el capítulo menciona Fried Phenotype como conjunto de variables que el
  sistema mide (fried_weight_loss, fried_weakness, etc.), eso ya no es cierto:
  el dataset nuevo no genera criterios de Fried directamente; usa proxies
  clínicos como `comorbidity_index`, `falls_last_12m`, `mmse_score`, etc.

---

## 2. Capítulo 3 — Dataset y fuentes de datos <a name="cap3"></a>

Este capítulo describe los esquemas de datos. Es el más afectado: el
generador (`generate_frailty.py`) fue completamente reescrito y las columnas
cambiaron en todas las fuentes.

### 2.1 Volumen de pacientes

| Memoria | Código actual |
|---------|---------------|
| "del orden de un millón de pacientes" | `TOTAL_PATIENTS = 100_000` (por defecto) |

El generador usa `TOTAL_PATIENTS = int(os.getenv("TOTAL_PATIENTS", "100000"))`.
La cifra de "un millón" es diez veces mayor que lo implementado.

### 2.2 Fuente A — Registros clínicos (`source_a/clinical_records/`)

**Columnas que la memoria describe y el código NO genera:**

| Columna descrita en memoria | Estado en código |
|-----------------------------|-----------------|
| `height_cm` | Eliminada |
| `weight_kg` | Eliminada |
| `heart_rate_bpm` | Eliminada |
| `fried_weight_loss` | Eliminada |
| `fried_weakness` | Eliminada |
| `fried_slowness` | Eliminada |
| `fried_low_activity` | Eliminada |
| `fried_exhaustion` | Eliminada |
| `tug_time_s` | Eliminada |
| `grip_strength_kg` | Eliminada |
| `gds` | Eliminada |
| `frailty_index_fi` | Eliminada |

**Columnas que el código genera y la memoria NO menciona:**

| Columna nueva | Descripción |
|---------------|-------------|
| `systolic_bp` | Presión sistólica (mmHg) |
| `diastolic_bp` | Presión diastólica (mmHg) |
| `gfr` | Tasa de filtración glomerular |
| `albumin` | Albúmina sérica (g/dL) |
| `hemoglobin` | Hemoglobina (g/dL) |
| `comorbidity_index` | Número de comorbilidades |
| `polypharmacy` | Número de fármacos |
| `falls_last_12m` | Caídas últimos 12 meses |
| `hospitalizations_last_12m` | Hospitalizaciones últimos 12 meses |
| `mmse_score` | Puntuación MMSE (cognitiva) |
| `depression_score` | Score de depresión (0.0–1.0) |

**Columna `sim_arrival_date`:** La memoria la lista en la tabla de Fuente A
como si fuera un campo del dataset. En el código **no es un campo de la
fuente**; se deriva en la capa Bronze a partir del campo watermark mediante
`F.to_date(F.col(DATE_COLS[source]))`. No existe en los archivos CSV
generados.

Referencia: [generate_frailty.py:104-137](../generate_frailty.py#L104-L137),
[ingest_clinical.py:109-114](../spark-apps/pipeline/bronze/ingest_clinical.py#L109-L114)

### 2.3 Fuente B1 — Encuestas SPPB (`source_b1/sppb_surveys/`)

**Diferencias de esquema:**

| Columna en memoria | En código | Observación |
|--------------------|-----------|-------------|
| `sppb_gait_speed` (int, 0–4) | `sppb_gait_speed_s` (float, segundos) | Tipo distinto: la memoria describe la puntuación SPPB (0–4); el código almacena el tiempo bruto en segundos |
| `sppb_chair_stand` (int, 0–4) | `sppb_chair_stand_s` (float, segundos) | Mismo problema |
| `fes_i_score` | — | Eliminada; no existe en el generador |
| `falls_last_year` | — | Eliminada; no existe en el generador |
| — | `sppb_total` | Campo nuevo: puntuación SPPB compuesta (0–12), calculado en el generador |

Referencia: [generate_frailty.py:140-157](../generate_frailty.py#L140-L157)

### 2.4 Fuente B2 — Encuestas de estilo de vida (`source_b2/lifestyle_surveys/`)

El esquema cambió **completamente**. No hay columnas compartidas entre lo
que describe la memoria y lo que genera el código.

**Columnas descritas en memoria (ninguna existe):**
`sedentary_hours_day`, `depression`, `hypertension`, `diabetes`, `arthritis`,
`num_chronic_conditions`, `physical_activity_vigorous`, `physical_activity_moderate`

**Columnas que genera el código (ninguna documentada):**

| Columna nueva | Descripción |
|---------------|-------------|
| `steps_per_day` | Pasos por día |
| `moderate_exercise_min_week` | Minutos de ejercicio moderado/semana |
| `protein_intake_g_per_kg` | Ingesta proteica (g/kg peso) |
| `social_contacts_per_week` | Contactos sociales semanales |
| `tobacco_use` | Tabaquismo (0=no, 1=exfumador, 2=activo) |
| `alcohol_units_per_week` | Unidades de alcohol/semana |

Referencia: [generate_frailty.py:160-178](../generate_frailty.py#L160-L178)

### 2.5 Fuente C — Eventos de marcha (`source_c/gait_events/`)

El esquema de cada stride cambió. Las columnas biomecánicas detalladas
fueron sustituidas por métricas de resumen.

**Columnas descritas en memoria y eliminadas:**

| Columna eliminada |
|-------------------|
| `stride_duration_s` |
| `swing_time_s` |
| `stance_time_s` |
| `foot_clearance_m` |
| `toe_off_angle_deg` |
| `heel_strike_angle_deg` |
| `lateral_excursion_m` |
| `stride_index` |

**Columnas nuevas no documentadas:**

| Columna nueva | Descripción |
|---------------|-------------|
| `stride_time_s` | Duración del paso (s) |
| `cadence_steps_min` | Cadencia (pasos/min) |
| `gait_speed_m_s` | Velocidad de marcha (m/s) |
| `asymmetry_index` | Índice de asimetría |
| `double_support_pct` | Porcentaje de doble apoyo |

Columnas que sí se mantienen: `event_id`, `patient_id`, `session_id`,
`session_timestamp`, `stride_length_m`.

Referencia: [generate_frailty.py:181-200](../generate_frailty.py#L181-L200)

### 2.6 Etiquetas (`labels/`)

La memoria describe 4 columnas: `patient_id`, `snapshot_date`,
`label_available_date`, `frailty_label`. El código añade una quinta:

| Columna nueva | Descripción |
|---------------|-------------|
| `updated_at` | Timestamp de llegada al pipeline (usada como columna watermark en Bronze) |

Esta columna es esencial para el funcionamiento del ingestor pero no aparece
en la tabla de la memoria.

Referencia: [generate_frailty.py:203-210](../generate_frailty.py#L203-L210),
[config.py:89](../spark-apps/pipeline/config.py#L89)

---

## 3. Capítulo 4 — Arquitectura del pipeline <a name="cap4"></a>

### 3.1 Estrategia de watermark (§4.2.1)

**Lo que describe la memoria:**
> El watermark avanza hasta `max(date_col)` de los registros procesados en
> cada tick. Si hay datos, el watermark salta al máximo; si no hay datos, el
> watermark no avanza.

**Lo que hace el código:**
Ventana **fija** de `INGEST_WINDOW_MINUTES` (por defecto 60 minutos de
tiempo simulado). En cada tick:
```
window_end = window_start + timedelta(minutes=INGEST_WINDOW_MINUTES)
```
El watermark avanza a `window_end` **independientemente de si hay datos** en
esa ventana. Si la ventana está vacía, el watermark avanza igualmente.

Esto es un cambio conceptual importante: la descripción de la memoria
correspondería a un watermark adaptativo; el código implementa un watermark
determinista de ventana fija.

Referencia: [ingest_clinical.py:65-106](../spark-apps/pipeline/bronze/ingest_clinical.py#L65-L106)

### 3.2 Columna watermark por fuente (§4.2.1)

**Lo que describe la memoria:**

| Fuente | Columna watermark según memoria |
|--------|--------------------------------|
| clinical | `snapshot_date` (fecha, precisión día) |
| sppb | `survey_date` |
| lifestyle | `survey_date` |
| gait | `session_timestamp` |
| labels | `label_available_date` (fecha disponibilidad diagnóstico) |

**Lo que usa el código (`DATE_COLS` en `config.py`):**

| Fuente | Columna watermark real |
|--------|----------------------|
| clinical | `updated_at` (timestamp de llegada, precisión minuto) |
| sppb | `survey_date` |
| lifestyle | `survey_date` |
| gait | `session_timestamp` |
| labels | `updated_at` (timestamp de llegada, precisión minuto) |

Las diferencias críticas son **clinical** (pasó de `snapshot_date` a
`updated_at`) y **labels** (pasó de `label_available_date` a `updated_at`).
En el nuevo diseño, el watermark siempre controla la **llegada al pipeline**,
no la fecha de evaluación clínica.

Referencia: [config.py:84-90](../spark-apps/pipeline/config.py#L84-L90)

### 3.3 Precisión de fechas (§4.2.1)

**Memoria:** "los registros clínicos y las etiquetas manejan fechas puras
(tipo DATE, sin hora), mientras que gait usa TIMESTAMP."

**Código:** Todas las fuentes usan `updated_at` o equivalente con precisión
de minuto (ISO 8601 con hora, p.ej. `2024-01-01T01:00:00Z`). No hay fechas
puras en el sistema de watermarks.

### 3.4 Origen de `sim_arrival_date` (§4.2.3)

**Memoria:** El campo `sim_arrival_date` se obtiene del reloj de simulación
(DAG `clock`), que avanza el tiempo simulado en cada tick.

**Código:** `sim_arrival_date` se deriva **de los propios datos**:
```python
df = df.withColumn("sim_arrival_date", F.to_date(F.col(date_col)))
```
Es `to_date(updated_at)` para clinical/labels, o `to_date(survey_date)`
para sppb/lifestyle. El DAG `clock` está **desactivado** (`schedule=None`).

Referencia: [ingest_clinical.py:114](../spark-apps/pipeline/bronze/ingest_clinical.py#L114),
[clock.py](../dags/clock.py)

### 3.5 Multi-cadencia por fuente (§4.4.1)

**Memoria:** Las cadencias de ingesta difieren deliberadamente por fuente
(gait se ingiere más frecuentemente; clínica y encuestas a intervalos más
espaciados).

**Código:** Los 5 DAGs de ingesta (`ingest_clinical.py`, `ingest_gait.py`,
`ingest_sppb.py`, `ingest_lifestyle.py`, `ingest_labels.py`) usan la
**misma variable** `SCHEDULE_INGEST` de `pipeline_config.py` (por defecto
`*/2 * * * *`). No hay diferenciación de cadencia por fuente.

Referencia: [pipeline_config.py](../dags/pipeline_config.py)

### 3.6 DAG `clock` y arquitectura de simulación (§4.4 general)

**Memoria:** El DAG `clock` avanza el tiempo de simulación y dispara el
pipeline. Los datos se publican progresivamente en la landing zone a medida
que avanza el reloj.

**Código:**
- `clock.py` tiene `schedule=None`; es un stub vacío marcado con `tags=["disabled"]`.
- Todos los datos sintéticos se depositan en MinIO **de una sola vez** antes
  de arrancar el pipeline mediante `upload_to_landing.py`.
- Los ingestores recorren los datos con ventanas fijas sin ningún reloj externo.

Referencia: [clock.py](../dags/clock.py),
[upload_to_landing.py](../spark-apps/scripts/upload_to_landing.py)

### 3.7 Condición anti-leakage en training table (§4.3.2)

**Memoria:** "La condición de la unión es que la fecha de disponibilidad de
la etiqueta sea anterior o igual al instante en que se procesa el registro."
Es decir, `label_available_date <= NOW()`.

**Código (`training_table.py:51-53`):**
```python
labels_clean = labels.filter(
    F.col("label_available_date") > F.col("snapshot_date")
)
```
El filtro verifica que la etiqueta fue confirmada **después** de la
evaluación (`label_available_date > snapshot_date`). No hay comparación
contra el tiempo de procesamiento actual. La barrera temporal frente a
etiquetas futuras la provee el watermark de `updated_at` en Bronze (aún no
se han ingestado etiquetas cuya llegada supere el watermark actual).

Estas son **dos lógicas distintas**:
- La descrita en la memoria (comparación contra `NOW()`) requeriría conocer
  el instante de proceso.
- La implementada (comparación con `snapshot_date`) garantiza que la etiqueta
  sea posterior a la evaluación clínica (anti-leakage de negocio), delegando
  la barrera de tiempo real en el watermark.

Referencia: [training_table.py:48-53](../spark-apps/pipeline/gold/training_table.py#L48-L53)

### 3.8 DAG monolítico `pipeline.py` (§4.4 diagrama)

**Memoria:** Puede describir un DAG único (`pipeline.py`) disparado por el
clock.

**Código:** `dags/pipeline.py` existe pero tiene `schedule=None` y es
obsoleto. El pipeline está dividido en DAGs independientes:
- `ingest_clinical.py`, `ingest_gait.py`, `ingest_sppb.py`,
  `ingest_lifestyle.py`, `ingest_labels.py` — ingesta Bronze→Silver.
- `reassemble.py` — Gold: gait_features → reassemble → training_table → inference.
- `train.py` — Champion-challenger disparado por volumen de etiquetas.

### 3.9 `ASSEMBLY_WAIT_DAYS` y escala de tiempo (§4.3.1)

**Memoria:** Puede mencionar ventanas de espera del orden de semanas o meses
(en tiempo calendario).

**Código:** `ASSEMBLY_WAIT_DAYS = 3` en **tiempo de pipeline** (días
simulados, no calendario). Con 100k pacientes a 10/min, 1 día simulado
equivale a 1.440 minutos reales de pipeline. El valor anterior era 45 días
calendario; 3 días de pipeline-time es una escala de tiempo radicalmente
distinta.

Referencia: [pipeline_config.py](../dags/pipeline_config.py),
[reassembler.py](../spark-apps/pipeline/gold/reassembler.py)

### 3.10 Cadena del DAG `reassemble`

**Posible descripción en memoria:** El reassembler ensambla pacientes y luego
pasa a la training table.

**Código actual (`reassemble.py`):** La cadena es:
```
gold_gait_features → gold_reassemble → gold_training → infer (opcional)
```
`gait_features` es un paso Gold nuevo que agrega métricas de sesión desde
Silver.GAIT antes del ensamblado. La inferencia se ejecuta al final de cada
tick; si no hay champion registrado en MLflow, se salta silenciosamente.

Referencia: [reassemble.py:85-90](../dags/reassemble.py#L85-L90)

### 3.11 Destino de las predicciones

**Clarificación (puede no estar en la memoria):** Las predicciones del
modelo champion se persisten en `GOLD.PREDICTIONS` (`s3a://gold/predictions`).
No se retroalimentan al ciclo de entrenamiento; son el output clínico final.

---

## 4. Bugs críticos — pipeline roto por incompatibilidad de esquema <a name="bugs"></a>

> **ATENCIÓN:** El pipeline no puede ejecutarse en su estado actual. El
> reescritor de `generate_frailty.py` cambió los esquemas de las 4 fuentes
> no-label, pero los transforms de Silver y el paso Gold de features no
> fueron actualizados. Todos los errores son `AnalysisException` de Spark
> (columna no encontrada) y romperán el DAG en el primer tick real.

### Bug 1 — `transform_clinical.py` (Silver)

**Fichero:** [spark-apps/pipeline/silver/transform_clinical.py:19-23](../spark-apps/pipeline/silver/transform_clinical.py#L19-L23)

El transform intenta castear 5 columnas de criterios Fried:
```python
.withColumn("fried_weight_loss",  F.col("fried_weight_loss").cast(IntegerType()))
.withColumn("fried_weakness",     F.col("fried_weakness").cast(IntegerType()))
.withColumn("fried_slowness",     F.col("fried_slowness").cast(IntegerType()))
.withColumn("fried_low_activity", F.col("fried_low_activity").cast(IntegerType()))
.withColumn("fried_exhaustion",   F.col("fried_exhaustion").cast(IntegerType()))
```

**Columnas que genera `generate_frailty.py`:** `patient_id`, `snapshot_date`,
`updated_at`, `age`, `sex`, `bmi`, `systolic_bp`, `diastolic_bp`, `gfr`,
`albumin`, `hemoglobin`, `comorbidity_index`, `polypharmacy`, `falls_last_12m`,
`hospitalizations_last_12m`, `mmse_score`, `depression_score`.

Ninguna de las 5 columnas Fried existe. Spark lanzará `AnalysisException`
al intentar resolver `F.col("fried_weight_loss")`.

**Corrección necesaria:** Eliminar los 5 `.withColumn` de criterios Fried
y, si es necesario, añadir casts para las nuevas columnas (`systolic_bp`,
`diastolic_bp`, `gfr`, etc.).

### Bug 2 — `transform_gait.py` (Silver)

**Fichero:** [spark-apps/pipeline/silver/transform_gait.py:15-23](../spark-apps/pipeline/silver/transform_gait.py#L15-L23)

```python
metric_cols = [
    "stride_duration_s", "stride_length_m", "swing_time_s", "stance_time_s",
    "foot_clearance_m", "toe_off_angle_deg", "heel_strike_angle_deg",
    "lateral_excursion_m",
]
df = df.withColumn("stride_index", F.col("stride_index").cast(IntegerType()))
for col in metric_cols:
    df = df.withColumn(col, F.col(col).cast(DoubleType()))
```

**Columnas que genera el código:** `event_id`, `patient_id`, `session_id`,
`session_timestamp`, `stride_length_m`, `stride_time_s`, `cadence_steps_min`,
`gait_speed_m_s`, `asymmetry_index`, `double_support_pct`.

Columnas no encontradas: `stride_duration_s`, `swing_time_s`, `stance_time_s`,
`foot_clearance_m`, `toe_off_angle_deg`, `heel_strike_angle_deg`,
`lateral_excursion_m`, `stride_index`. Ocho columnas ausentes.

**Corrección necesaria:** Reemplazar `metric_cols` con las columnas nuevas
(`stride_time_s`, `cadence_steps_min`, `gait_speed_m_s`, `asymmetry_index`,
`double_support_pct`) y eliminar el cast de `stride_index`.

### Bug 3 — `transform_sppb.py` (Silver)

**Fichero:** [spark-apps/pipeline/silver/transform_sppb.py:25-28](../spark-apps/pipeline/silver/transform_sppb.py#L25-L28)

```python
.withColumn("sppb_gait_speed",  F.col("sppb_gait_speed").cast(IntegerType()))
.withColumn("sppb_chair_stand", F.col("sppb_chair_stand").cast(IntegerType()))
.withColumn("falls_last_year",  F.col("falls_last_year").cast(IntegerType()))
```

**Problemas:**
1. `sppb_gait_speed` → en el generador se llama `sppb_gait_speed_s` (con sufijo `_s`)
2. `sppb_chair_stand` → en el generador se llama `sppb_chair_stand_s`
3. `falls_last_year` → no existe en el generador (solo hay `falls_last_12m` en clinical)
4. El cast a `IntegerType()` es incorrecto: el generador produce valores `float` (segundos brutos)

**Corrección necesaria:** Cambiar los nombres a `sppb_gait_speed_s` y
`sppb_chair_stand_s`, usar `DoubleType()`, y eliminar el cast de
`falls_last_year`.

### Bug 4 — `transform_lifestyle.py` (Silver)

**Fichero:** [spark-apps/pipeline/silver/transform_lifestyle.py:24-31](../spark-apps/pipeline/silver/transform_lifestyle.py#L24-L31)

```python
.withColumn("sedentary_hours_day",          F.col(...).cast(DoubleType()))
.withColumn("depression",                   F.col(...).cast(IntegerType()))
.withColumn("hypertension",                 F.col(...).cast(IntegerType()))
.withColumn("diabetes",                     F.col(...).cast(IntegerType()))
.withColumn("arthritis",                    F.col(...).cast(IntegerType()))
.withColumn("num_chronic_conditions",       F.col(...).cast(IntegerType()))
.withColumn("physical_activity_vigorous",   F.col(...).cast(IntegerType()))
.withColumn("physical_activity_moderate",   F.col(...).cast(IntegerType()))
```

**Columnas que genera el código:** `steps_per_day`, `moderate_exercise_min_week`,
`protein_intake_g_per_kg`, `social_contacts_per_week`, `tobacco_use`,
`alcohol_units_per_week`.

Ninguna de las 8 columnas casteadas existe. Ocho `AnalysisException` en
cascada.

**Corrección necesaria:** Reemplazar todo el bloque de casts por los de las
columnas nuevas (`steps_per_day→IntegerType`, `moderate_exercise_min_week→IntegerType`,
`protein_intake_g_per_kg→DoubleType`, `social_contacts_per_week→IntegerType`,
`tobacco_use→IntegerType`, `alcohol_units_per_week→IntegerType`).

### Bug 5 — `gait_features.py` (Gold)

**Fichero:** [spark-apps/pipeline/gold/gait_features.py:19-33](../spark-apps/pipeline/gold/gait_features.py#L19-L33)

```python
F.mean(F.col("stride_length_m") / F.col("stride_duration_s")).alias("gait_velocity_ms"),
F.mean("stride_duration_s").alias("stride_time_s"),
(F.lit(60.0) / F.mean("stride_duration_s")).alias("cadence_strides_min"),
(F.mean(F.col("swing_time_s") / F.col("stride_duration_s")) * 100).alias("swing_time_pct"),
F.mean("foot_clearance_m").alias("foot_clearance_m"),
F.mean("toe_off_angle_deg").alias("toe_off_angle_deg"),
F.mean("heel_strike_angle_deg").alias("heel_strike_angle_deg"),
F.mean("lateral_excursion_m").alias("lateral_excursion_m"),
(F.stddev("stride_duration_s") / F.mean("stride_duration_s")).alias("stride_time_cv"),
```

Todas las referencias a `stride_duration_s`, `swing_time_s`, `foot_clearance_m`,
`toe_off_angle_deg`, `heel_strike_angle_deg`, `lateral_excursion_m` producirán
`AnalysisException` porque esas columnas no existen en Silver.GAIT.

**Corrección necesaria:** Reescribir las agregaciones usando las columnas
disponibles: `stride_length_m`, `stride_time_s`, `cadence_steps_min`,
`gait_speed_m_s`, `asymmetry_index`, `double_support_pct`. Por ejemplo:
```python
F.mean("gait_speed_m_s").alias("gait_velocity_ms"),
F.mean("stride_time_s").alias("stride_time_s"),
F.mean("cadence_steps_min").alias("cadence_steps_min"),
F.mean("asymmetry_index").alias("asymmetry_index"),
F.mean("double_support_pct").alias("double_support_pct"),
(F.stddev("stride_time_s") / F.mean("stride_time_s")).alias("stride_time_cv"),
```

---

## Resumen de acciones requeridas

### Para actualizar la memoria

| Sección | Acción |
|---------|--------|
| §3.1 Volumen | Cambiar "millón" a 100.000 (configurable via `TOTAL_PATIENTS`) |
| §3.x Tabla Fuente A | Actualizar esquema completo (eliminar Fried/altura/peso; añadir variables clínicas nuevas) |
| §3.x Tabla Fuente B1 | Cambiar `sppb_gait_speed`/`sppb_chair_stand` (int 0–4) a `_s` (float segundos); eliminar `fes_i_score`, `falls_last_year`; añadir `sppb_total` |
| §3.x Tabla Fuente B2 | Reemplazar esquema completo de lifestyle |
| §3.x Tabla Fuente C | Reemplazar columnas biomecánicas por métricas de resumen |
| §3.x Labels | Añadir `updated_at` y explicar su rol como watermark |
| §3.x `sim_arrival_date` | Clarificar que es columna derivada en Bronze, no campo de fuente |
| §4.2.1 Watermark | Describir ventana fija (`INGEST_WINDOW_MINUTES`), no max-based |
| §4.2.1 DATE_COLS | Actualizar columnas: clinical→`updated_at`, labels→`updated_at` |
| §4.2.1 Precisión | Aclarar que todas las fuentes usan timestamps de minuto |
| §4.2.3 `sim_arrival_date` | Origen es `to_date(updated_at)` de los datos, no el clock DAG |
| §4.4.1 Multi-cadencia | Describir cadencia única `SCHEDULE_INGEST` para todas las fuentes |
| §4.4 Clock DAG | Eliminar; sustituir por descripción de `upload_to_landing.py` |
| §4.3.2 Anti-leakage | Corregir condición: `label_available_date > snapshot_date` (no comparación con NOW) |
| §4.4 DAG estructura | Actualizar diagrama: 5 DAGs ingesta + reassemble + train_challenger |
| §4.3.1 ASSEMBLY_WAIT_DAYS | Aclarar unidad: 3 días de pipeline-time (no calendario) |

### Para arreglar el código (bugs críticos)

| Fichero | Corrección |
|---------|-----------|
| `spark-apps/pipeline/silver/transform_clinical.py` | Eliminar 5 casts Fried; añadir casts columnas nuevas |
| `spark-apps/pipeline/silver/transform_gait.py` | Reemplazar `metric_cols` y eliminar `stride_index` |
| `spark-apps/pipeline/silver/transform_sppb.py` | Renombrar a `_s`, cambiar tipo a Double, eliminar `falls_last_year` |
| `spark-apps/pipeline/silver/transform_lifestyle.py` | Reemplazar los 8 casts con las 6 columnas nuevas |
| `spark-apps/pipeline/gold/gait_features.py` | Reescribir agregaciones con columnas disponibles |
