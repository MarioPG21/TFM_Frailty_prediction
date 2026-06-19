from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, TimestampType

from pipeline.config import BRONZE, SILVER
from pipeline.rules import get_rules
from pipeline.silver.quarantine import apply_rules_and_split


def run(spark: SparkSession) -> None:
    df = spark.read.format("delta").load(BRONZE.GAIT)

    metric_cols = [
        "stride_length_m", "stride_time_s", "cadence_steps_min",
        "gait_speed_m_s", "asymmetry_index", "double_support_pct",
    ]
    df = df.withColumn("session_timestamp", F.col("session_timestamp").cast(TimestampType()))
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
