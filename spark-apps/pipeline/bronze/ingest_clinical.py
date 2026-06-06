from __future__ import annotations

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from pipeline.config import BRONZE, DEDUP_KEYS, DATE_COLS, LANDING
from pipeline.bronze.watermark import read_watermark, write_watermark


def _merge_or_create(spark, df, path, merge_cond, partition_cols=None):
    if DeltaTable.isDeltaTable(spark, path):
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(df.alias("s"), merge_cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        w = df.write.format("delta").mode("overwrite")
        if partition_cols:
            w = w.partitionBy(*partition_cols)
        w.save(path)


def _ingest_batch(
    spark: SparkSession,
    source: str,
    landing_path: str,
    bronze_path: str,
    file_format: str,   # "csv" or "json"
) -> None:
    date_col = DATE_COLS[source]
    wm = read_watermark(spark, source)

    try:
        reader = spark.read.option("recursiveFileLookup", "true")
        if file_format == "csv":
            df = reader.option("header", "true").option("inferSchema", "true").csv(landing_path)
        else:
            df = reader.json(landing_path)
    except Exception as e:
        print(f"[{source}] No se pudo leer {landing_path}: {e}")
        return

    if df.rdd.isEmpty():
        print(f"[{source}] Landing vacío, nada que ingestar.")
        return

    if wm:
        df = df.filter(F.col(date_col) > wm)

    if df.rdd.isEmpty():
        print(f"[{source}] Sin registros nuevos (watermark={wm}).")
        return

    # Audit columns
    df = (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )

    # Partition columns from the date field
    df = (
        df
        .withColumn("_d", F.to_date(F.col(date_col)))
        .withColumn("year",  F.year(F.col("_d")))
        .withColumn("month", F.month(F.col("_d")))
        .drop("_d")
    )

    df = df.cache()
    n = df.count()

    keys = DEDUP_KEYS[source]
    merge_cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    _merge_or_create(spark, df, bronze_path, merge_cond, partition_cols=["year", "month"])

    max_val = df.agg(F.max(date_col).alias("m")).first()["m"]
    if max_val:
        write_watermark(spark, source, str(max_val))

    df.unpersist()
    print(f"[{source}] Bronze: {n:,} registros nuevos. Watermark → {max_val}")


def run_clinical(spark: SparkSession) -> None:
    _ingest_batch(spark, "clinical", LANDING.CLINICAL, BRONZE.CLINICAL, "csv")


def run_sppb(spark: SparkSession) -> None:
    _ingest_batch(spark, "sppb", LANDING.SPPB, BRONZE.SPPB, "json")


def run_lifestyle(spark: SparkSession) -> None:
    _ingest_batch(spark, "lifestyle", LANDING.LIFESTYLE, BRONZE.LIFESTYLE, "json")


def run_labels(spark: SparkSession) -> None:
    _ingest_batch(spark, "labels", LANDING.LABELS, BRONZE.LABELS, "csv")


def run(spark: SparkSession) -> None:
    run_clinical(spark)
    run_sppb(spark)
    run_lifestyle(spark)
    run_labels(spark)


if __name__ == "__main__":
    from pipeline.spark_session import get_spark
    _spark = get_spark("bronze-clinical")
    run(_spark)
    _spark.stop()
