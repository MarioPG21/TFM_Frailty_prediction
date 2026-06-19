"""
Bronze ingestor — todas las fuentes excepto gait.

Lógica de ventana fija
-----------------------
En lugar de avanzar el watermark hasta max(date_col), cada tick procesa
exactamente INGEST_WINDOW_MINUTES de tiempo simulado y avanza el watermark
esa cantidad fija, con o sin datos. Esto garantiza que:

  • Cada tick consume la misma "rodaja" de tiempo simulado.
  • Si un tick no tiene datos (ventana sin registros), el watermark avanza
    igualmente y el siguiente tick ve la ventana correcta.
  • La cadencia de ingesta es determinista e independiente de la densidad
    de datos en cada ventana.

Watermark almacenado como ISO 8601 (e.g. "2024-01-01T01:00:00").

sim_arrival_date
----------------
Se deriva del campo DATE_COLS[source] de los propios datos (no de --sim-day).
Representa el día de pipeline en que llegó el registro al sistema, utilizado
por el reassembler para la ventana de cuarentena de inferencia.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from pipeline.config import BRONZE, DATE_COLS, DEDUP_KEYS, INGEST_WINDOW_MINUTES, LANDING
from pipeline.bronze.watermark import read_watermark, write_watermark

_TS_FMT = "%Y-%m-%dT%H:%M:%S"


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
    file_format: str,   # "csv" o "json"
) -> None:
    date_col = DATE_COLS[source]
    wm_str   = read_watermark(spark, source)

    # ── Calcular ventana ──────────────────────────────────────────────────────
    if wm_str:
        window_start = datetime.strptime(wm_str[:19], _TS_FMT)
        window_end   = window_start + timedelta(minutes=INGEST_WINDOW_MINUTES)
    else:
        window_start = None  # primera ejecución: no filtra por inicio
        window_end   = None  # primera ejecución: recoge todo hasta el primer watermark

    # ── Leer landing ──────────────────────────────────────────────────────────
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
        print(f"[{source}] Landing vacío.")
        return

    # ── Filtrar por ventana fija ──────────────────────────────────────────────
    if window_start and window_end:
        df = df.filter(
            (F.col(date_col) > F.lit(window_start.strftime(_TS_FMT)).cast("timestamp"))
            & (F.col(date_col) <= F.lit(window_end.strftime(_TS_FMT)).cast("timestamp"))
        )
    elif window_start:
        # Sin window_end calculado (no debería ocurrir, pero por seguridad)
        df = df.filter(F.col(date_col) > F.lit(window_start.strftime(_TS_FMT)).cast("timestamp"))
    # Si window_start es None (primera ejecución), no filtramos: recoge el
    # primer bloque de datos y luego fija el watermark al máximo de ese bloque.

    n = df.count() if not df.rdd.isEmpty() else 0
    if n == 0:
        # Ventana sin datos — avanzar watermark igualmente (ventana fija)
        new_wm = window_end.strftime(_TS_FMT) if window_end else datetime.now().strftime(_TS_FMT)
        write_watermark(spark, source, new_wm)
        print(f"[{source}] Ventana sin datos. Watermark → {new_wm}")
        return

    # ── Columnas de auditoría ─────────────────────────────────────────────────
    # sim_arrival_date: día de pipeline del registro (derivado de los datos)
    df = (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file",         F.input_file_name())
        .withColumn("sim_arrival_date",    F.to_date(F.col(date_col)))
    )

    # Particionado por year/month del campo watermark
    df = (
        df
        .withColumn("_d",    F.to_date(F.col(date_col)))
        .withColumn("year",  F.year("_d"))
        .withColumn("month", F.month("_d"))
        .drop("_d")
    )

    df = df.cache()
    df.count()  # materializar caché

    # ── MERGE a Bronze ────────────────────────────────────────────────────────
    keys       = DEDUP_KEYS[source]
    merge_cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    _merge_or_create(spark, df, bronze_path, merge_cond, partition_cols=["year", "month"])

    # ── Avanzar watermark (ventana fija: window_end, no max(date_col)) ───────
    if window_end:
        new_wm = window_end.strftime(_TS_FMT)
    else:
        # Primera ejecución: usar el máximo del bloque como nuevo watermark
        raw = df.agg(F.max(date_col).alias("m")).first()["m"]
        new_wm = str(raw)[:19].replace(" ", "T")
    write_watermark(spark, source, new_wm)

    df.unpersist()
    print(
        f"[{source}] Bronze: {n:,} registros. "
        f"Ventana [{window_start} → {window_end or 'primera'}]. "
        f"Watermark → {new_wm}"
    )


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
