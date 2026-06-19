from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

from pipeline.config import BRONZE, SILVER
from pipeline.rules import get_rules
from pipeline.silver.quarantine import apply_rules_and_split


def run(spark: SparkSession) -> None:
    try:
        df = spark.read.format("delta").load(BRONZE.LIFESTYLE)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            print(f"[lifestyle] Silver: sin datos en bronze todavía, omitiendo")
            return
        raise

    df = (
        df
        .withColumn("survey_date",                   F.col("survey_date").cast(TimestampType()))
        .withColumn("steps_per_day",                 F.col("steps_per_day").cast(IntegerType()))
        .withColumn("moderate_exercise_min_week",    F.col("moderate_exercise_min_week").cast(IntegerType()))
        .withColumn("protein_intake_g_per_kg",       F.col("protein_intake_g_per_kg").cast(DoubleType()))
        .withColumn("social_contacts_per_week",      F.col("social_contacts_per_week").cast(IntegerType()))
        .withColumn("tobacco_use",                   F.col("tobacco_use").cast(IntegerType()))
        .withColumn("alcohol_units_per_week",        F.col("alcohol_units_per_week").cast(IntegerType()))
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
