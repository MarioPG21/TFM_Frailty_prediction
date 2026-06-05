from __future__ import annotations

from datetime import datetime, timezone

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from pipeline.config import BRONZE


def read_watermark(spark: SparkSession, source: str) -> str | None:
    """Returns the last processed value for source, or None on the first run."""
    if not DeltaTable.isDeltaTable(spark, BRONZE.WATERMARKS):
        return None
    df = spark.read.format("delta").load(BRONZE.WATERMARKS)
    row = df.filter(F.col("source") == source).select("last_processed").first()
    return row["last_processed"] if row else None


def write_watermark(spark: SparkSession, source: str, value: str) -> None:
    """Upserts the watermark for source. Call only after the job completes successfully."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    new_row = spark.createDataFrame(
        [(source, value, now)],
        schema="source STRING, last_processed STRING, updated_at TIMESTAMP",
    )
    if DeltaTable.isDeltaTable(spark, BRONZE.WATERMARKS):
        (
            DeltaTable.forPath(spark, BRONZE.WATERMARKS)
            .alias("t")
            .merge(new_row.alias("s"), "t.source = s.source")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        new_row.write.format("delta").save(BRONZE.WATERMARKS)
