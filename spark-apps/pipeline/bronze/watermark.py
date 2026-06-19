from __future__ import annotations

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from pipeline.config import BRONZE

# Cada fuente tiene su propia tabla Delta en _control/watermarks_<source>
# (misma carpeta padre que BRONZE.WATERMARKS pero con sufijo, no subdirectorio)
# para evitar que Delta Lake confunda las tablas per-source con particiones Hive
# de un directorio padre, o conflictos con la tabla compartida antigua.

def _path(source: str) -> str:
    return f"{BRONZE.WATERMARKS}_{source}"


def read_watermark(spark: SparkSession, source: str) -> str | None:
    """Devuelve el último valor procesado para source, o None en la primera ejecución."""
    path = _path(source)
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    df = spark.read.format("delta").load(path)
    row = df.select("last_processed").first()
    return row["last_processed"] if row else None


def write_watermark(spark: SparkSession, source: str, value: str) -> None:
    """Sobreescribe el watermark de source. Cada fuente tiene su propia tabla."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    new_row = spark.createDataFrame(
        [(source, value, now)],
        schema="source STRING, last_processed STRING, updated_at TIMESTAMP",
    )
    (
        new_row.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(_path(source))
    )
