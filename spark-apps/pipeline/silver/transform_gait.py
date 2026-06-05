from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

from pipeline.config import BRONZE, SILVER
from pipeline.rules import get_rules
from pipeline.silver.quarantine import apply_rules_and_split


def run(spark: SparkSession) -> None:
    df = spark.read.format("delta").load(BRONZE.GAIT)

    metric_cols = [
        "stride_duration_s", "stride_length_m", "swing_time_s", "stance_time_s",
        "foot_clearance_m", "toe_off_angle_deg", "heel_strike_angle_deg",
        "lateral_excursion_m",
    ]
    df = df.withColumn("session_timestamp", F.col("session_timestamp").cast(TimestampType()))
    df = df.withColumn("stride_index", F.col("stride_index").cast(IntegerType()))
    for col in metric_cols:
        df = df.withColumn(col, F.col(col).cast(DoubleType()))

    rules = get_rules("gait")
    df_valid, df_quarantine = apply_rules_and_split(df, rules, "gait")

    _write(df_valid,      SILVER.GAIT)
    _write(df_quarantine, SILVER.QUARANTINE_GAIT)
    print(f"[gait] Silver: {df_valid.count():,} válidos  "
          f"{df_quarantine.count():,} cuarentena")


def _write(df, path):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("year", "month")
        .save(path)
    )


if __name__ == "__main__":
    from pipeline.spark_session import get_spark
    _spark = get_spark("silver-gait")
    run(_spark)
    _spark.stop()
