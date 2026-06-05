from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

from pipeline.config import GOLD, SILVER


def _asof_join_latest(
    base: DataFrame,
    lookup: DataFrame,
    pid_col: str,
    base_date: str,
    lookup_date: str,
    feature_cols: list[str],
) -> DataFrame:
    """
    Left-join base with the most recent lookup record (by lookup_date)
    that is on or before base_date, per patient.

    feature_cols: columns from lookup to include in the result (excluding pid_col
    and lookup_date which are only used for joining/ordering).
    """
    # Prefix lookup_date to avoid name collisions after join
    lk = lookup.select(
        F.col(pid_col).alias("_lk_pid"),
        F.col(lookup_date).alias("_lk_date"),
        *[F.col(c) for c in feature_cols],
    )

    joined = (
        base
        .join(
            lk,
            (base[pid_col] == lk["_lk_pid"]) &
            (lk["_lk_date"] <= base[base_date]),
            "left",
        )
        .drop("_lk_pid")
    )

    w = Window.partitionBy(pid_col, base_date).orderBy(F.desc("_lk_date"))

    return (
        joined
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_lk_date")
    )


def run(spark: SparkSession) -> None:
    clinical  = spark.read.format("delta").load(SILVER.CLINICAL)
    gait      = spark.read.format("delta").load(GOLD.GAIT_FEATURES)
    sppb      = spark.read.format("delta").load(SILVER.SPPB)
    lifestyle = spark.read.format("delta").load(SILVER.LIFESTYLE)

    # Columns to carry from each source (excluding keys and audit cols)
    _audit = {"ingestion_timestamp", "source_file", "year", "month"}

    gait_cols = [c for c in gait.columns
                 if c not in {"patient_id", "session_id", "session_date"} | _audit]

    sppb_cols = [c for c in sppb.columns
                 if c not in {"patient_id", "response_id", "survey_date"} | _audit]

    lifestyle_cols = [c for c in lifestyle.columns
                      if c not in {"patient_id", "response_id", "survey_date"} | _audit]

    # gait: session_date is already DateType
    training = _asof_join_latest(
        clinical, gait,
        pid_col="patient_id", base_date="snapshot_date",
        lookup_date="session_date", feature_cols=gait_cols,
    )

    # sppb: survey_date is TimestampType — cast to DateType for consistent comparison
    sppb = sppb.withColumn("survey_date", F.to_date("survey_date"))
    training = _asof_join_latest(
        training, sppb,
        pid_col="patient_id", base_date="snapshot_date",
        lookup_date="survey_date", feature_cols=sppb_cols,
    )

    # lifestyle: same cast
    lifestyle = lifestyle.withColumn("survey_date", F.to_date("survey_date"))
    training = _asof_join_latest(
        training, lifestyle,
        pid_col="patient_id", base_date="snapshot_date",
        lookup_date="survey_date", feature_cols=lifestyle_cols,
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
