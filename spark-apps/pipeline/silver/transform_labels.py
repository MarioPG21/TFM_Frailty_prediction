from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType

from pipeline.config import BRONZE, SILVER
from pipeline.rules import get_rules
from pipeline.silver.quarantine import apply_rules_and_split


def run(spark: SparkSession) -> None:
    df = spark.read.format("delta").load(BRONZE.LABELS)

    df = (
        df
        .withColumn("snapshot_date",        F.col("snapshot_date").cast(DateType()))
        .withColumn("label_available_date",  F.col("label_available_date").cast(DateType()))
        .withColumn("frailty_label",         F.col("frailty_label").cast(IntegerType()))
    )

    rules = get_rules("labels")
    df_valid, df_quarantine = apply_rules_and_split(df, rules, "labels")

    _write(df_valid,      SILVER.LABELS)
    _write(df_quarantine, SILVER.QUARANTINE_LABELS)
    print(f"[labels] Silver: {df_valid.count():,} válidos  "
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
    _spark = get_spark("silver-labels")
    run(_spark)
    _spark.stop()
