from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, TimestampType

from pipeline.config import BRONZE, SILVER
from pipeline.rules import get_rules
from pipeline.silver.quarantine import apply_rules_and_split


def run(spark: SparkSession) -> None:
    df = spark.read.format("delta").load(BRONZE.CLINICAL)

    df = (
        df
        .withColumn("snapshot_date",      F.col("snapshot_date").cast(DateType()))
        .withColumn("updated_at",         F.col("updated_at").cast(TimestampType()))
        .withColumn("fried_weight_loss",  F.col("fried_weight_loss").cast(IntegerType()))
        .withColumn("fried_weakness",     F.col("fried_weakness").cast(IntegerType()))
        .withColumn("fried_slowness",     F.col("fried_slowness").cast(IntegerType()))
        .withColumn("fried_low_activity", F.col("fried_low_activity").cast(IntegerType()))
        .withColumn("fried_exhaustion",   F.col("fried_exhaustion").cast(IntegerType()))
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
