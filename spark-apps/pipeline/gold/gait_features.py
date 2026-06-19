from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from pipeline.config import GOLD, SILVER


def run(spark: SparkSession) -> None:
    try:
        df = spark.read.format("delta").load(SILVER.GAIT)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            print("[gait_features] Silver.GAIT sin datos todavía, omitiendo.")
            return
        raise

    features = (
        df.groupBy(
            "patient_id",
            "session_id",
            F.to_date("session_timestamp").alias("session_date"),
        )
        .agg(
            F.mean("gait_speed_m_s").alias("gait_velocity_ms"),
            F.mean("stride_length_m").alias("stride_length_m"),
            F.mean("stride_time_s").alias("stride_time_s"),
            F.mean("cadence_steps_min").alias("cadence_steps_min"),
            F.mean("asymmetry_index").alias("asymmetry_index"),
            F.mean("double_support_pct").alias("double_support_pct"),
            (F.stddev("stride_time_s") / F.mean("stride_time_s"))
             .alias("stride_time_cv"),
            F.count("*").alias("n_strides"),
        )
    )

    (
        features.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD.GAIT_FEATURES)
    )
    print(f"[gait_features] Gold: {features.count():,} sesiones")


if __name__ == "__main__":
    from pipeline.spark_session import get_spark
    _spark = get_spark("gold-gait-features")
    run(_spark)
    _spark.stop()
