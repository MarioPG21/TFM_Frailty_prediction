from __future__ import annotations

import json

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

from pipeline.config import BRONZE, KAFKA_BOOTSTRAP, KAFKA_TOPIC_GAIT
from pipeline.bronze.watermark import read_watermark, write_watermark

_VALUE_SCHEMA = StructType([
    StructField("event_id",              StringType(),  False),
    StructField("patient_id",            StringType(),  False),
    StructField("session_id",            StringType(),  False),
    StructField("session_timestamp",     StringType(),  False),
    StructField("stride_index",          IntegerType(), True),
    StructField("stride_duration_s",     DoubleType(),  True),
    StructField("stride_length_m",       DoubleType(),  True),
    StructField("swing_time_s",          DoubleType(),  True),
    StructField("stance_time_s",         DoubleType(),  True),
    StructField("foot_clearance_m",      DoubleType(),  True),
    StructField("toe_off_angle_deg",     DoubleType(),  True),
    StructField("heel_strike_angle_deg", DoubleType(),  True),
    StructField("lateral_excursion_m",   DoubleType(),  True),
])


def run(spark: SparkSession) -> None:
    wm = read_watermark(spark, "gait")

    if wm and wm.startswith("offset:"):
        start = int(wm.split(":")[1]) + 1
        starting_offsets = json.dumps({KAFKA_TOPIC_GAIT: {"0": start}})
    else:
        starting_offsets = "earliest"

    try:
        raw = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC_GAIT)
            .option("startingOffsets", starting_offsets)
            .option("endingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
            .cache()
        )
    except Exception as e:
        print(f"[gait] Error conectando a Kafka ({KAFKA_BOOTSTRAP}): {e}")
        return

    if raw.rdd.isEmpty():
        raw.unpersist()
        print("[gait] Sin mensajes nuevos en Kafka.")
        return

    max_offset = raw.agg(F.max("offset").alias("m")).first()["m"]

    df = (
        raw
        .select(
            F.from_json(F.col("value").cast("string"), _VALUE_SCHEMA).alias("v"),
            F.col("offset").alias("_offset"),
        )
        .select("v.*", "_offset")
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file",
                    F.concat(F.lit(f"kafka:{KAFKA_TOPIC_GAIT}:"),
                             F.col("_offset").cast("string")))
        .drop("_offset")
        .withColumn("_d",    F.to_date(F.col("session_timestamp")))
        .withColumn("year",  F.year("_d"))
        .withColumn("month", F.month("_d"))
        .drop("_d")
        .cache()
    )
    raw.unpersist()

    n = df.count()

    if DeltaTable.isDeltaTable(spark, BRONZE.GAIT):
        (
            DeltaTable.forPath(spark, BRONZE.GAIT)
            .alias("t")
            .merge(df.alias("s"), "t.event_id = s.event_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").partitionBy("year", "month").save(BRONZE.GAIT)

    write_watermark(spark, "gait", f"offset:{max_offset}")
    df.unpersist()
    print(f"[gait] Bronze: {n:,} eventos. Watermark → offset:{max_offset}")


if __name__ == "__main__":
    from pipeline.spark_session import get_spark
    _spark = get_spark("bronze-gait")
    run(_spark)
    _spark.stop()
