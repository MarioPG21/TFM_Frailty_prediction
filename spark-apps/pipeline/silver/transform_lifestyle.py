from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

from pipeline.config import BRONZE, SILVER
from pipeline.rules import get_rules
from pipeline.silver.quarantine import apply_rules_and_split


def run(spark: SparkSession) -> None:
    df = spark.read.format("delta").load(BRONZE.LIFESTYLE)

    df = (
        df
        .withColumn("survey_date",                 F.col("survey_date").cast(TimestampType()))
        .withColumn("sedentary_hours_day",          F.col("sedentary_hours_day").cast(DoubleType()))
        .withColumn("depression",                   F.col("depression").cast(IntegerType()))
        .withColumn("hypertension",                 F.col("hypertension").cast(IntegerType()))
        .withColumn("diabetes",                     F.col("diabetes").cast(IntegerType()))
        .withColumn("arthritis",                    F.col("arthritis").cast(IntegerType()))
        .withColumn("num_chronic_conditions",       F.col("num_chronic_conditions").cast(IntegerType()))
        .withColumn("physical_activity_vigorous",   F.col("physical_activity_vigorous").cast(IntegerType()))
        .withColumn("physical_activity_moderate",   F.col("physical_activity_moderate").cast(IntegerType()))
    )

    rules = get_rules("lifestyle")
    df_valid, df_quarantine = apply_rules_and_split(df, rules, "lifestyle")

    _write(df_valid,      SILVER.LIFESTYLE)
    _write(df_quarantine, SILVER.QUARANTINE_LIFESTYLE)
    print(f"[lifestyle] Silver: {df_valid.count():,} válidos  "
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
    _spark = get_spark("silver-lifestyle")
    run(_spark)
    _spark.stop()
