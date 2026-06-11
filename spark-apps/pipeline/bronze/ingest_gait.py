from __future__ import annotations

from pyspark.sql import SparkSession

from pipeline.config import BRONZE, LANDING
from pipeline.bronze.ingest_clinical import _ingest_batch


def run(spark: SparkSession) -> None:
    _ingest_batch(spark, "gait", LANDING.GAIT, BRONZE.GAIT, "json")


if __name__ == "__main__":
    from pipeline.spark_session import get_spark
    _spark = get_spark("bronze-gait")
    run(_spark)
    _spark.stop()
