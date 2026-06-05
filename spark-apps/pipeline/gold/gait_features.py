from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from pipeline.config import GOLD, SILVER


def run(spark: SparkSession) -> None:
    df = spark.read.format("delta").load(SILVER.GAIT)

    features = (
        df.groupBy(
            "patient_id",
            "session_id",
            F.to_date("session_timestamp").alias("session_date"),
        )
        .agg(
            F.mean(F.col("stride_length_m") / F.col("stride_duration_s"))
             .alias("gait_velocity_ms"),
            F.mean("stride_length_m").alias("stride_length_m"),
            F.mean("stride_duration_s").alias("stride_time_s"),
            (F.lit(60.0) / F.mean("stride_duration_s")).alias("cadence_strides_min"),
            (F.mean(F.col("swing_time_s") / F.col("stride_duration_s")) * 100)
             .alias("swing_time_pct"),
            F.mean("foot_clearance_m").alias("foot_clearance_m"),
            F.mean("toe_off_angle_deg").alias("toe_off_angle_deg"),
            F.mean("heel_strike_angle_deg").alias("heel_strike_angle_deg"),
            F.mean("lateral_excursion_m").alias("lateral_excursion_m"),
            (F.stddev("stride_duration_s") / F.mean("stride_duration_s"))
             .alias("stride_time_cv"),
            F.count("*").alias("n_strides"),
        )
        .withColumn("stance_time_pct", F.lit(100.0) - F.col("swing_time_pct"))
        .withColumn("step_speed_ms",   F.col("stride_length_m") / F.col("stride_time_s"))
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
