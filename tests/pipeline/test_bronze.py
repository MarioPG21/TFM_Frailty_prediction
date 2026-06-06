"""
test_bronze.py — verifica la integridad y el comportamiento de la capa Bronze.

Precondición: los scripts publish_*.py deben haberse ejecutado con --ticks all
antes de lanzar estos tests.
"""
import pytest
from pyspark.sql import functions as F

from pipeline.config import BRONZE, LANDING
from pipeline.bronze.ingest_clinical import run as run_clinical_bronze, run_labels as run_labels_bronze
from pipeline.bronze.ingest_gait import run as run_gait_bronze
from pipeline.bronze.watermark import read_watermark


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count(spark, path):
    return spark.read.format("delta").load(path).count()


def _bronze_run(spark):
    run_clinical_bronze(spark)
    run_gait_bronze(spark)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestBronzeClinical:
    def test_rows_at_least_as_many_as_source(self, spark):
        """Bronze tiene al menos tantas filas como los CSV en landing."""
        n_landing = (
            spark.read
            .option("recursiveFileLookup", "true")
            .option("header", "true")
            .csv(LANDING.CLINICAL)
            .count()
        )
        _bronze_run(spark)
        n_bronze = _count(spark, BRONZE.CLINICAL)
        assert n_bronze >= n_landing, (
            f"Bronze ({n_bronze}) tiene menos filas que landing ({n_landing})"
        )

    def test_no_duplicates(self, spark):
        df = spark.read.format("delta").load(BRONZE.CLINICAL)
        total = df.count()
        distinct = df.select("patient_id", "snapshot_date").distinct().count()
        assert total == distinct, f"Hay duplicados en bronze_clinical: {total} vs {distinct}"

    def test_audit_columns_not_null(self, spark):
        df = spark.read.format("delta").load(BRONZE.CLINICAL)
        nulls = df.filter(
            F.col("ingestion_timestamp").isNull() | F.col("source_file").isNull()
        ).count()
        assert nulls == 0, f"{nulls} filas con columnas de auditoría nulas"

    def test_watermark_updated(self, spark):
        wm = read_watermark(spark, "clinical")
        assert wm is not None, "El watermark de clinical no fue escrito"
        assert wm > "2024-01-01", f"Watermark inesperadamente bajo: {wm}"

    def test_idempotent(self, spark):
        n_before = _count(spark, BRONZE.CLINICAL)
        _bronze_run(spark)
        n_after = _count(spark, BRONZE.CLINICAL)
        assert n_before == n_after, (
            f"Bronze no es idempotente: {n_before} → {n_after}"
        )


class TestBronzeSppb:
    def test_no_duplicates(self, spark):
        _bronze_run(spark)
        df = spark.read.format("delta").load(BRONZE.SPPB)
        total = df.count()
        distinct = df.select("response_id").distinct().count()
        assert total == distinct

    def test_audit_columns_not_null(self, spark):
        df = spark.read.format("delta").load(BRONZE.SPPB)
        nulls = df.filter(
            F.col("ingestion_timestamp").isNull() | F.col("source_file").isNull()
        ).count()
        assert nulls == 0


class TestBronzeGait:
    def test_no_duplicates(self, spark):
        _bronze_run(spark)
        df = spark.read.format("delta").load(BRONZE.GAIT)
        total = df.count()
        distinct = df.select("event_id").distinct().count()
        assert total == distinct, f"Duplicados en bronze_gait: {total} vs {distinct}"

    def test_audit_columns_not_null(self, spark):
        df = spark.read.format("delta").load(BRONZE.GAIT)
        nulls = df.filter(
            F.col("ingestion_timestamp").isNull() | F.col("source_file").isNull()
        ).count()
        assert nulls == 0

    def test_watermark_has_offset(self, spark):
        wm = read_watermark(spark, "gait")
        assert wm is not None
        assert wm.startswith("offset:"), f"Formato de watermark gait inesperado: {wm}"

    def test_idempotent(self, spark):
        n_before = _count(spark, BRONZE.GAIT)
        _bronze_run(spark)
        n_after = _count(spark, BRONZE.GAIT)
        assert n_before == n_after


class TestBronzeLabels:
    def test_no_duplicates(self, spark):
        run_labels_bronze(spark)
        df = spark.read.format("delta").load(BRONZE.LABELS)
        total = df.count()
        distinct = df.select("patient_id", "snapshot_date").distinct().count()
        assert total == distinct, f"Duplicados en bronze_labels: {total} vs {distinct}"

    def test_audit_columns_not_null(self, spark):
        df = spark.read.format("delta").load(BRONZE.LABELS)
        nulls = df.filter(
            F.col("ingestion_timestamp").isNull() | F.col("source_file").isNull()
        ).count()
        assert nulls == 0

    def test_watermark_updated(self, spark):
        wm = read_watermark(spark, "labels")
        assert wm is not None, "El watermark de labels no fue escrito"

    def test_rows_match_clinical(self, spark):
        n_labels   = _count(spark, BRONZE.LABELS)
        n_clinical = _count(spark, BRONZE.CLINICAL)
        assert n_labels == n_clinical, (
            f"bronze_labels ({n_labels}) ≠ bronze_clinical ({n_clinical}): "
            "debe haber un label por cada snapshot clínico"
        )

    def test_idempotent(self, spark):
        n_before = _count(spark, BRONZE.LABELS)
        run_labels_bronze(spark)
        n_after = _count(spark, BRONZE.LABELS)
        assert n_before == n_after
