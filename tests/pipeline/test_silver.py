"""
test_silver.py — verifica la capa Silver: tipado, reglas y cuarentena.
"""
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, TimestampType

from pipeline.config import BRONZE, SILVER
from pipeline.silver.transform_clinical  import run as run_clinical
from pipeline.silver.transform_sppb      import run as run_sppb
from pipeline.silver.transform_lifestyle import run as run_lifestyle
from pipeline.silver.transform_gait      import run as run_gait
from pipeline.silver.transform_labels    import run as run_labels
from pipeline.rules import get_rules
from pipeline.silver.quarantine import apply_rules_and_split


def _run_silver(spark):
    run_clinical(spark)
    run_sppb(spark)
    run_lifestyle(spark)
    run_gait(spark)
    run_labels(spark)


def _count(spark, path):
    return spark.read.format("delta").load(path).count()


class TestSilverClinical:
    def test_valid_plus_quarantine_equals_bronze(self, spark):
        _run_silver(spark)
        n_bronze     = _count(spark, BRONZE.CLINICAL)
        n_valid      = _count(spark, SILVER.CLINICAL)
        n_quarantine = _count(spark, SILVER.QUARANTINE_CLINICAL)
        assert n_valid + n_quarantine == n_bronze, (
            f"Silver ({n_valid}) + cuarentena ({n_quarantine}) ≠ Bronze ({n_bronze})"
        )

    def test_no_rule_violations_in_silver(self, spark):
        df = spark.read.format("delta").load(SILVER.CLINICAL)
        rules = get_rules("clinical")
        for name, constraint in rules.items():
            violations = df.filter(~F.expr(constraint)).count()
            assert violations == 0, (
                f"Regla '{name}' violada en silver_clinical: {violations} filas"
            )

    def test_types_after_transform(self, spark):
        schema = {f.name: f.dataType
                  for f in spark.read.format("delta").load(SILVER.CLINICAL).schema}
        assert isinstance(schema["snapshot_date"],     DateType)
        assert isinstance(schema["updated_at"],        TimestampType)
        assert isinstance(schema["fried_weight_loss"], IntegerType)
        # frailty_label is no longer in Source A — verify it is absent
        assert "frailty_label" not in schema, (
            "frailty_label no debe estar en silver_clinical tras la migración"
        )

    def test_quarantine_has_failed_rules_column(self, spark):
        df_q = spark.read.format("delta").load(SILVER.QUARANTINE_CLINICAL)
        assert "failed_rules" in df_q.columns

    def test_quarantine_triggered_by_invalid_data(self, spark):
        """Inyecta una fila inválida y verifica que va a cuarentena."""
        bronze = spark.read.format("delta").load(BRONZE.CLINICAL).limit(1)
        invalid = bronze.withColumn("age", F.lit(150))  # fuera de rango [65, 95]
        rules = get_rules("clinical")
        _, q = apply_rules_and_split(invalid, rules, "clinical")
        assert q.count() == 1
        failed = q.select("failed_rules").first()["failed_rules"]
        assert "valid_age" in failed


class TestSilverSppb:
    def test_valid_plus_quarantine_equals_bronze(self, spark):
        _run_silver(spark)
        assert (_count(spark, SILVER.SPPB) + _count(spark, SILVER.QUARANTINE_SPPB)
                == _count(spark, BRONZE.SPPB))

    def test_no_rule_violations_in_silver(self, spark):
        df = spark.read.format("delta").load(SILVER.SPPB)
        for name, constraint in get_rules("sppb").items():
            violations = df.filter(~F.expr(constraint)).count()
            assert violations == 0, f"Regla '{name}' violada en silver_sppb"

    def test_survey_date_is_timestamp(self, spark):
        schema = {f.name: f.dataType
                  for f in spark.read.format("delta").load(SILVER.SPPB).schema}
        assert isinstance(schema["survey_date"], TimestampType)


class TestSilverLifestyle:
    def test_valid_plus_quarantine_equals_bronze(self, spark):
        _run_silver(spark)
        assert (_count(spark, SILVER.LIFESTYLE) + _count(spark, SILVER.QUARANTINE_LIFESTYLE)
                == _count(spark, BRONZE.LIFESTYLE))

    def test_no_rule_violations_in_silver(self, spark):
        df = spark.read.format("delta").load(SILVER.LIFESTYLE)
        for name, constraint in get_rules("lifestyle").items():
            violations = df.filter(~F.expr(constraint)).count()
            assert violations == 0, f"Regla '{name}' violada en silver_lifestyle"


class TestSilverGait:
    def test_valid_plus_quarantine_equals_bronze(self, spark):
        _run_silver(spark)
        assert (_count(spark, SILVER.GAIT) + _count(spark, SILVER.QUARANTINE_GAIT)
                == _count(spark, BRONZE.GAIT))

    def test_no_rule_violations_in_silver(self, spark):
        df = spark.read.format("delta").load(SILVER.GAIT)
        for name, constraint in get_rules("gait").items():
            violations = df.filter(~F.expr(constraint)).count()
            assert violations == 0, f"Regla '{name}' violada en silver_gait"

    def test_metric_cols_are_double(self, spark):
        schema = {f.name: f.dataType
                  for f in spark.read.format("delta").load(SILVER.GAIT).schema}
        for col in ("stride_duration_s", "stride_length_m", "swing_time_s"):
            assert isinstance(schema[col], DoubleType), f"{col} no es DoubleType"


class TestSilverLabels:
    def test_valid_plus_quarantine_equals_bronze(self, spark):
        _run_silver(spark)
        n_bronze     = _count(spark, BRONZE.LABELS)
        n_valid      = _count(spark, SILVER.LABELS)
        n_quarantine = _count(spark, SILVER.QUARANTINE_LABELS)
        assert n_valid + n_quarantine == n_bronze, (
            f"Silver ({n_valid}) + cuarentena ({n_quarantine}) ≠ Bronze ({n_bronze})"
        )

    def test_no_rule_violations_in_silver(self, spark):
        df = spark.read.format("delta").load(SILVER.LABELS)
        for name, constraint in get_rules("labels").items():
            violations = df.filter(~F.expr(constraint)).count()
            assert violations == 0, f"Regla '{name}' violada en silver_labels"

    def test_types_after_transform(self, spark):
        schema = {f.name: f.dataType
                  for f in spark.read.format("delta").load(SILVER.LABELS).schema}
        assert isinstance(schema["snapshot_date"],       DateType)
        assert isinstance(schema["label_available_date"], DateType)
        assert isinstance(schema["frailty_label"],        IntegerType)

    def test_quarantine_has_failed_rules_column(self, spark):
        df_q = spark.read.format("delta").load(SILVER.QUARANTINE_LABELS)
        assert "failed_rules" in df_q.columns

    def test_temporal_coherence_violation_quarantined(self, spark):
        """
        Inyecta una fila con label_available_date <= snapshot_date
        y verifica que temporal_coherence la manda a cuarentena.
        """
        bronze = spark.read.format("delta").load(BRONZE.LABELS).limit(1)
        # Set label_available_date = snapshot_date (equal, not strictly after)
        invalid = bronze.withColumn(
            "label_available_date", F.col("snapshot_date")
        )
        rules = get_rules("labels")
        _, q = apply_rules_and_split(invalid, rules, "labels")
        assert q.count() == 1
        failed = q.select("failed_rules").first()["failed_rules"]
        assert "temporal_coherence" in failed
