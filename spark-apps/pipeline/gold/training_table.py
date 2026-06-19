from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from pipeline.config import GOLD, SILVER


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


def run(spark: SparkSession) -> None:
    # Clinical es la base del join; sin ella no hay nada que ensamblar.
    try:
        clinical = spark.read.format("delta").load(SILVER.CLINICAL)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            print("[gold_training] Silver.CLINICAL sin datos todavía, omitiendo.")
            return
        raise

    gait      = _read_or_empty(spark, GOLD.GAIT_FEATURES, "patient_id", "session_date")
    sppb      = _read_or_empty(spark, SILVER.SPPB,        "patient_id", "survey_date")
    lifestyle = _read_or_empty(spark, SILVER.LIFESTYLE,   "patient_id", "survey_date")
    labels    = _read_or_empty(spark, SILVER.LABELS,      "patient_id", "snapshot_date")

    # sim_arrival_date se excluye de fuentes secundarias para evitar colisión
    # de nombre con la columna homónima de clinical (que es la canónica para
    # la ventana de cuarentena del reassembler).
    _audit = {"ingestion_timestamp", "source_file", "year", "month", "sim_arrival_date", "updated_at"}

    gait_cols = [c for c in gait.columns
                 if c not in {"patient_id", "session_id", "session_date"} | _audit]

    sppb_cols = [c for c in sppb.columns
                 if c not in {"patient_id", "response_id", "survey_date"} | _audit]

    lifestyle_cols = [c for c in lifestyle.columns
                      if c not in {"patient_id", "response_id", "survey_date"} | _audit]

    label_cols = [c for c in labels.columns
                  if c not in {"patient_id", "snapshot_date"} | _audit]

    # Anti-leakage: la etiqueta debe haber sido confirmada DESPUÉS de la evaluación.
    # El guard evita AnalysisException cuando labels es un DataFrame vacío sin
    # columnas de negocio (todavía no ha llegado ninguna etiqueta).
    if "label_available_date" in labels.columns:
        labels_clean = labels.filter(
            F.col("label_available_date") > F.col("snapshot_date")
        )
    else:
        labels_clean = labels

    # Join simple por patient_id: cada paciente tiene exactamente un registro
    # por fuente. LEFT JOIN para conservar pacientes con encuestas aún pendientes.
    training = clinical

    training = training.join(
        gait.select("patient_id", *gait_cols),
        "patient_id",
        "left",
    )
    training = training.join(
        sppb.select("patient_id", *sppb_cols),
        "patient_id",
        "left",
    )
    training = training.join(
        lifestyle.select("patient_id", *lifestyle_cols),
        "patient_id",
        "left",
    )
    training = training.join(
        labels_clean.select("patient_id", *label_cols),
        "patient_id",
        "left",
    )

    (
        training.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD.TRAINING)
    )
    print(f"[gold_training] Gold: {training.count():,} filas  "
          f"{len(training.columns)} columnas")


if __name__ == "__main__":
    from pipeline.spark_session import get_spark
    _spark = get_spark("gold-training")
    run(_spark)
    _spark.stop()
