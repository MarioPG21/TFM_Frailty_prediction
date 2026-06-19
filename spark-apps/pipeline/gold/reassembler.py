"""
Reassembler — Gold layer
========================
Detecta pacientes con las 4 fuentes completas, los ensambla en GOLD.ASSEMBLED
y envía a GOLD.INFERENCE_QUARANTINE a los que llevan más de ASSEMBLY_WAIT_DAYS
días sin completarse.

Diseño:
- Idempotente: pacientes ya ensamblados no se vuelven a procesar.
- Sin collect(): usa anti-joins en lugar de .isin(collect()) para escalar a 1M.
- ASSEMBLY_WAIT_DAYS se mide en tiempo simulado anclado a sim_arrival_date
  (fecha en que el lote llegó al sistema), NO a snapshot_date (fecha de
  evaluación). Esto evita que lotes con fechas de evaluación adelantadas
  desplacen el cutoff y manden a cuarentena pacientes aún dentro de plazo.
- Rescate de cuarentena: en cada tick se eliminan de GOLD.INFERENCE_QUARANTINE
  los pacientes que ya tienen las 4 fuentes (sus datos llegaron tarde pero
  están ahora completos).
"""
from __future__ import annotations

from datetime import timedelta

import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

import os as _os

from pipeline.config import GOLD, SILVER

# Configurable desde ASSEMBLY_WAIT_DAYS env var (días de pipeline-time basados
# en sim_arrival_date = to_date(DATE_COLS[source]) de los datos propios).
ASSEMBLY_WAIT_DAYS: int = int(_os.getenv("ASSEMBLY_WAIT_DAYS", "3"))


def _read_or_empty(spark: SparkSession, path: str, pid_col: str, date_col: str) -> DataFrame:
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


def _merge_or_append(spark: SparkSession, df: DataFrame, path: str) -> None:
    """Escribe df a path: MERGE si ya existe la tabla Delta, append si no."""
    if DeltaTable.isDeltaTable(spark, path):
        dt = DeltaTable.forPath(spark, path)
        (
            dt.alias("existing")
            .merge(df.alias("new"), "existing.patient_id = new.patient_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            df.write
            .format("delta")
            .mode("append")
            .save(path)
        )


def _assemble(
    spark: SparkSession,
    pids: DataFrame,
    clinical: DataFrame,
    gait: DataFrame,
    sppb: DataFrame,
    lifestyle: DataFrame,
) -> DataFrame:
    """Construye la fila de evaluación completa para cada patient_id en pids."""
    # sim_arrival_date se excluye de fuentes secundarias: la columna canónica
    # para la ventana de cuarentena proviene de clinical.
    _audit = {"ingestion_timestamp", "source_file", "year", "month", "sim_arrival_date", "updated_at"}

    gait_cols = [c for c in gait.columns
                 if c not in {"patient_id", "session_id", "session_date"} | _audit]
    sppb_cols = [c for c in sppb.columns
                 if c not in {"patient_id", "response_id", "survey_date"} | _audit]
    ls_cols   = [c for c in lifestyle.columns
                 if c not in {"patient_id", "response_id", "survey_date"} | _audit]

    return (
        pids
        .join(clinical,                              "patient_id", "inner")
        .join(gait.select("patient_id", *gait_cols), "patient_id", "inner")
        .join(sppb.select("patient_id", *sppb_cols), "patient_id", "inner")
        .join(lifestyle.select("patient_id", *ls_cols), "patient_id", "inner")
        .withColumn("assembly_timestamp", F.current_timestamp())
    )


def _handle_quarantine(
    spark: SparkSession,
    clinical: DataFrame,
    pids_gait: DataFrame,
    pids_sppb: DataFrame,
    pids_lifestyle: DataFrame,
) -> None:
    """
    Pacientes cuyo sim_arrival_date (cuándo llegaron al sistema en tiempo simulado)
    supera ASSEMBLY_WAIT_DAYS y todavía les falta alguna fuente
    → GOLD.INFERENCE_QUARANTINE.

    Se ancla a sim_arrival_date (no a snapshot_date) para que lotes con fechas
    de evaluación adelantadas no desplacen el cutoff y no manden a cuarentena
    pacientes que todavía están dentro del plazo de espera.

    Usa anti-joins (sin collect) para escalar a 1M pacientes.
    """
    max_date_row = clinical.agg(F.max("sim_arrival_date")).first()
    if max_date_row is None or max_date_row[0] is None:
        return

    max_arrival = max_date_row[0]
    cutoff = max_arrival - timedelta(days=ASSEMBLY_WAIT_DAYS)

    old_patients = clinical.filter(
        F.col("sim_arrival_date") <= F.lit(cutoff.isoformat()).cast("date")
    ).select("patient_id")

    # Pacientes viejos que faltan en al menos una fuente
    incomplete = (
        old_patients.join(pids_gait,      "patient_id", "left_anti")
        .union(old_patients.join(pids_sppb,      "patient_id", "left_anti"))
        .union(old_patients.join(pids_lifestyle, "patient_id", "left_anti"))
        .distinct()
    )

    n_incomplete = incomplete.count()
    if n_incomplete == 0:
        return

    # Construir fila de cuarentena con flags de qué falta.
    # Usa LEFT JOINs con indicador booleano (sin collect) para escalar a 1M.
    quarantine = (
        incomplete
        .join(clinical, "patient_id", "inner")
        .join(pids_gait.withColumn("_has_gait", F.lit(True)),
              "patient_id", "left")
        .join(pids_sppb.withColumn("_has_sppb", F.lit(True)),
              "patient_id", "left")
        .join(pids_lifestyle.withColumn("_has_ls", F.lit(True)),
              "patient_id", "left")
        .withColumn("missing_gait",      F.col("_has_gait").isNull())
        .withColumn("missing_sppb",      F.col("_has_sppb").isNull())
        .withColumn("missing_lifestyle", F.col("_has_ls").isNull())
        .drop("_has_gait", "_has_sppb", "_has_ls")
        .withColumn("quarantine_timestamp", F.current_timestamp())
    )

    # Excluir ya en cuarentena
    if DeltaTable.isDeltaTable(spark, GOLD.INFERENCE_QUARANTINE):
        already_q = (
            spark.read.format("delta").load(GOLD.INFERENCE_QUARANTINE)
            .select("patient_id")
        )
        quarantine = quarantine.join(already_q, "patient_id", "left_anti")

    n_new = quarantine.count()
    if n_new > 0:
        _merge_or_append(spark, quarantine, GOLD.INFERENCE_QUARANTINE)
        print(f"[reassembler] Cuarentena de inferencia: {n_new:,} pacientes nuevos")


def _rescue_from_quarantine(spark: SparkSession, pids_complete: DataFrame) -> None:
    """
    Elimina de GOLD.INFERENCE_QUARANTINE los pacientes que ahora tienen las 4
    fuentes completas: sus datos llegaron con retraso pero ya están disponibles.
    Usa whenMatchedDelete() — sin collect(), escala a 1M pacientes.
    """
    if not DeltaTable.isDeltaTable(spark, GOLD.INFERENCE_QUARANTINE):
        return

    q_df = spark.read.format("delta").load(GOLD.INFERENCE_QUARANTINE)
    n_to_rescue = q_df.join(pids_complete, "patient_id", "inner").count()
    if n_to_rescue == 0:
        return

    (
        DeltaTable.forPath(spark, GOLD.INFERENCE_QUARANTINE)
        .alias("q")
        .merge(pids_complete.alias("c"), "q.patient_id = c.patient_id")
        .whenMatchedDelete()
        .execute()
    )
    print(f"[reassembler] Cuarentena: {n_to_rescue:,} pacientes rescatados (datos ya completos)")


def run(spark: SparkSession) -> None:
    """
    Por cada tick del pipeline:
    1. Lee los 4 Silver tables + Gold.GAIT_FEATURES.
    2. Determina qué pacientes tienen las 4 fuentes presentes.
    3. Excluye los ya ensamblados (idempotencia).
    4. Escribe nuevos completos a GOLD.ASSEMBLED.
    5. Rescata de GOLD.INFERENCE_QUARANTINE los ahora completos.
    6. Detecta incompletos fuera de ventana → GOLD.INFERENCE_QUARANTINE.
    """
    try:
        clinical = spark.read.format("delta").load(SILVER.CLINICAL)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            print("[reassembler] Silver.CLINICAL sin datos todavía, omitiendo.")
            return
        raise

    gait      = _read_or_empty(spark, GOLD.GAIT_FEATURES, "patient_id", "session_date")
    sppb      = _read_or_empty(spark, SILVER.SPPB,        "patient_id", "survey_date")
    lifestyle = _read_or_empty(spark, SILVER.LIFESTYLE,   "patient_id", "survey_date")

    pids_clinical  = clinical.select("patient_id").distinct()
    pids_gait      = gait.select("patient_id").distinct()
    pids_sppb      = sppb.select("patient_id").distinct()
    pids_lifestyle = lifestyle.select("patient_id").distinct()

    # Pacientes con las 4 fuentes presentes (antes de filtrar ya-ensamblados,
    # necesario para el rescate de cuarentena)
    pids_complete_all = (
        pids_clinical
        .join(pids_gait,      "patient_id", "inner")
        .join(pids_sppb,      "patient_id", "inner")
        .join(pids_lifestyle, "patient_id", "inner")
    )

    # Excluir los ya ensamblados para no duplicar
    pids_to_assemble = pids_complete_all
    if DeltaTable.isDeltaTable(spark, GOLD.ASSEMBLED):
        already = (
            spark.read.format("delta").load(GOLD.ASSEMBLED)
            .select("patient_id")
        )
        pids_to_assemble = pids_complete_all.join(already, "patient_id", "left_anti")

    n_new = pids_to_assemble.count()
    if n_new > 0:
        assembled = _assemble(spark, pids_to_assemble, clinical, gait, sppb, lifestyle)
        _merge_or_append(spark, assembled, GOLD.ASSEMBLED)
        print(f"[reassembler] Ensamblados: {n_new:,} pacientes nuevos")
    else:
        print("[reassembler] Sin pacientes nuevos completos en este tick")

    # Purgar cuarentena: los que ahora tienen las 4 fuentes ya no necesitan esperar
    _rescue_from_quarantine(spark, pids_complete_all)

    _handle_quarantine(spark, clinical, pids_gait, pids_sppb, pids_lifestyle)


if __name__ == "__main__":
    from pipeline.spark_session import get_spark
    _spark = get_spark("gold-reassembler")
    run(_spark)
    _spark.stop()
