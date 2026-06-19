from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DoubleType, IntegerType, TimestampType

from pipeline.config import BRONZE, SILVER
from pipeline.rules import get_rules
from pipeline.silver.quarantine import apply_rules_and_split


def run(spark: SparkSession) -> None:
    df = spark.read.format("delta").load(BRONZE.CLINICAL)

    df = (
        df
        .withColumn("snapshot_date",               F.col("snapshot_date").cast(DateType()))
        .withColumn("updated_at",                  F.col("updated_at").cast(TimestampType()))
        .withColumn("age",                         F.col("age").cast(IntegerType()))
        .withColumn("bmi",                         F.col("bmi").cast(DoubleType()))
        .withColumn("systolic_bp",                 F.col("systolic_bp").cast(IntegerType()))
        .withColumn("diastolic_bp",                F.col("diastolic_bp").cast(IntegerType()))
        .withColumn("gfr",                         F.col("gfr").cast(DoubleType()))
        .withColumn("albumin",                     F.col("albumin").cast(DoubleType()))
        .withColumn("hemoglobin",                  F.col("hemoglobin").cast(DoubleType()))
        .withColumn("comorbidity_index",           F.col("comorbidity_index").cast(IntegerType()))
        .withColumn("polypharmacy",                F.col("polypharmacy").cast(IntegerType()))
        .withColumn("falls_last_12m",              F.col("falls_last_12m").cast(IntegerType()))
        .withColumn("hospitalizations_last_12m",   F.col("hospitalizations_last_12m").cast(IntegerType()))
        .withColumn("mmse_score",                  F.col("mmse_score").cast(IntegerType()))
        .withColumn("depression_score",            F.col("depression_score").cast(DoubleType()))
    )

    rules = get_rules("clinical")
    df_valid, df_quarantine = apply_rules_and_split(df, rules, "clinical")

    _write(df_valid,     SILVER.CLINICAL)
    _write(df_quarantine, SILVER.QUARANTINE_CLINICAL)
    print(f"[clinical] Silver: {df_valid.count():,} válidos  "
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
    _spark = get_spark("silver-clinical")
    run(_spark)
    _spark.stop()
