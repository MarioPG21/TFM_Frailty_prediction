from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, TimestampType

from pipeline.config import BRONZE, SILVER
from pipeline.rules import get_rules
from pipeline.silver.quarantine import apply_rules_and_split


def run(spark: SparkSession) -> None:
    try:
        df = spark.read.format("delta").load(BRONZE.SPPB)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            print(f"[sppb] Silver: sin datos en bronze todavía, omitiendo")
            return
        raise

    df = (
        df
        .withColumn("survey_date",      F.col("survey_date").cast(TimestampType()))
        .withColumn("sppb_balance",     F.col("sppb_balance").cast(IntegerType()))
        .withColumn("sppb_gait_speed",  F.col("sppb_gait_speed").cast(IntegerType()))
        .withColumn("sppb_chair_stand", F.col("sppb_chair_stand").cast(IntegerType()))
        .withColumn("sppb_total",       F.col("sppb_total").cast(IntegerType()))
        .withColumn("falls_last_year",  F.col("falls_last_year").cast(IntegerType()))
    )

    rules = get_rules("sppb")
    df_valid, df_quarantine = apply_rules_and_split(df, rules, "sppb")

    _write(df_valid,      SILVER.SPPB)
    _write(df_quarantine, SILVER.QUARANTINE_SPPB)
    print(f"[sppb] Silver: {df_valid.count():,} válidos  "
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
    _spark = get_spark("silver-sppb")
    run(_spark)
    _spark.stop()
