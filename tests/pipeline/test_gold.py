"""
test_gold.py — verifica la capa Gold: agregaciones y tabla de entrenamiento.
"""
import pytest
from pyspark.sql import functions as F

from pipeline.config import GOLD, SILVER
from pipeline.gold.gait_features  import run as run_gait_features
from pipeline.gold.reassembler    import run as run_reassembler
from pipeline.gold.training_table import run as run_training


def _run_gold(spark):
    run_gait_features(spark)
    run_training(spark)
    run_reassembler(spark)


def _count(spark, path):
    return spark.read.format("delta").load(path).count()


class TestGoldGaitFeatures:
    def test_one_row_per_session(self, spark):
        _run_gold(spark)
        df = spark.read.format("delta").load(GOLD.GAIT_FEATURES)
        total    = df.count()
        distinct = df.select("session_id").distinct().count()
        assert total == distinct, (
            f"gold_gait_features no tiene una fila por sesión: {total} filas, "
            f"{distinct} session_ids distintos"
        )

    def test_stride_time_cv_positive(self, spark):
        df = spark.read.format("delta").load(GOLD.GAIT_FEATURES)
        neg = df.filter(F.col("stride_time_cv") <= 0).count()
        assert neg == 0, f"{neg} filas con stride_time_cv ≤ 0"

    def test_expected_columns_present(self, spark):
        cols = set(spark.read.format("delta").load(GOLD.GAIT_FEATURES).columns)
        expected = {
            "patient_id", "session_id", "session_date",
            "gait_velocity_ms", "stride_length_m", "stride_time_s",
            "cadence_strides_min", "swing_time_pct", "stance_time_pct",
            "foot_clearance_m", "toe_off_angle_deg", "heel_strike_angle_deg",
            "lateral_excursion_m", "stride_time_cv", "step_speed_ms", "n_strides",
        }
        missing = expected - cols
        assert not missing, f"Columnas faltantes en gold_gait_features: {missing}"

    def test_n_strides_positive(self, spark):
        df = spark.read.format("delta").load(GOLD.GAIT_FEATURES)
        bad = df.filter(F.col("n_strides") <= 0).count()
        assert bad == 0


class TestGoldTraining:
    def test_at_least_one_row_per_patient(self, spark):
        _run_gold(spark)
        training  = spark.read.format("delta").load(GOLD.TRAINING)
        clinical  = spark.read.format("delta").load(SILVER.CLINICAL)
        n_patients_training = training.select("patient_id").distinct().count()
        n_patients_clinical = clinical.select("patient_id").distinct().count()
        assert n_patients_training == n_patients_clinical, (
            f"gold_training cubre {n_patients_training} pacientes, "
            f"silver_clinical tiene {n_patients_clinical}"
        )

    def test_frailty_label_mostly_present(self, spark):
        """
        Pacientes evaluados recientemente pueden no tener label aún
        (label_available_date = evaluation_date + 7-30 días). Se acepta hasta
        un 15% de nulos, correspondiente a pacientes en los últimos 30 días.
        """
        df = spark.read.format("delta").load(GOLD.TRAINING)
        total = df.count()
        nulls = df.filter(F.col("frailty_label").isNull()).count()
        assert nulls / total < 0.15, (
            f"{nulls} de {total} filas ({nulls/total:.1%}) sin frailty_label — "
            "supera el 15% esperado para evaluaciones recientes sin label aún"
        )

    def test_no_label_leakage(self, spark):
        """
        En el nuevo paradigma, la etiqueta siempre llega DESPUÉS de la evaluación
        (label_available_date > snapshot_date). Verifica que el filtro anti-leakage
        de training_table.py excluyó cualquier etiqueta con fecha anterior o igual
        a la evaluación (que indicaría data leakage).
        """
        df = spark.read.format("delta").load(GOLD.TRAINING)
        leakage = df.filter(
            F.col("label_available_date").isNotNull() &
            (F.col("label_available_date") <= F.col("snapshot_date"))
        ).count()
        assert leakage == 0, (
            f"{leakage} filas donde el label llegó ANTES o EN la evaluación (data leakage)"
        )

    def test_column_count(self, spark):
        df = spark.read.format("delta").load(GOLD.TRAINING)
        # clinical (~23 cols) + gait features (13) + sppb (6) + lifestyle (9) + labels (2)
        assert len(df.columns) >= 40, (
            f"gold_training tiene solo {len(df.columns)} columnas"
        )

    def test_rows_match_clinical(self, spark):
        n_training = _count(spark, GOLD.TRAINING)
        n_clinical  = _count(spark, SILVER.CLINICAL)
        assert n_training == n_clinical, (
            f"gold_training ({n_training}) ≠ silver_clinical ({n_clinical})"
        )


class TestGoldAssembled:
    def test_no_null_features(self, spark):
        """
        Todo paciente ensamblado tiene las 4 fuentes completas: no debe
        haber nulls en columnas de gait, sppb ni lifestyle.
        """
        _run_gold(spark)
        df = spark.read.format("delta").load(GOLD.ASSEMBLED)
        for col in ("gait_velocity_ms", "sppb_total", "sedentary_hours_day"):
            nulls = df.filter(F.col(col).isNull()).count()
            assert nulls == 0, f"Columna {col} tiene nulls en Gold.ASSEMBLED"

    def test_one_row_per_patient(self, spark):
        """Un paciente ensamblado aparece exactamente una vez."""
        df = spark.read.format("delta").load(GOLD.ASSEMBLED)
        total    = df.count()
        distinct = df.select("patient_id").distinct().count()
        assert total == distinct, "Gold.ASSEMBLED tiene pacientes duplicados"
