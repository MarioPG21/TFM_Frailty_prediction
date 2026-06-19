"""
DAG de entrenamiento champion-challenger.

Dispara un retrain cuando el número de pacientes etiquetados nuevos en
GOLD.TRAINING supera RETRAIN_THRESHOLD respecto al último entrenamiento.

El conteo de "último entrenamiento" se almacena en la Variable de Airflow
`last_trained_count` (int, por defecto 0).

Configuración: dags/pipeline_config.py
"""
from __future__ import annotations

import subprocess
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from pipeline_config import RETRAIN_THRESHOLD, SCHEDULE_TRAIN_CHECK


@dag(
    dag_id="train_challenger",
    schedule=SCHEDULE_TRAIN_CHECK,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["training", "champion-challenger"],
)
def train_challenger_dag():

    @task
    def check_and_train() -> None:
        """
        1. Cuenta pacientes etiquetados en GOLD.TRAINING (via deltalake).
        2. Si (count - last_trained_count) >= RETRAIN_THRESHOLD → entrena.
        3. Actualiza last_trained_count tras entrenamiento exitoso.
        """
        import os

        from deltalake import DeltaTable

        storage_opts = {
            "AWS_ENDPOINT_URL":                 os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            "AWS_ACCESS_KEY_ID":                os.environ["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY":            os.environ["AWS_SECRET_ACCESS_KEY"],
            "AWS_ALLOW_HTTP":                   "true",
            "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
        }
        gold_bucket  = os.environ.get("MINIO_BUCKET_GOLD", "gold")
        training_path = f"s3://{gold_bucket}/training"

        # Contar pacientes etiquetados disponibles en GOLD.TRAINING
        try:
            dt = DeltaTable(training_path, storage_options=storage_opts)
            df = dt.to_pandas()
            # Solo filas con etiqueta (training table tiene LEFT JOIN con labels)
            labeled = int(df["frailty_label"].notna().sum())
        except Exception as e:
            print(f"[train] GOLD.TRAINING no disponible aún: {e}")
            raise AirflowSkipException("GOLD.TRAINING no disponible todavía.")

        last_count = int(Variable.get("last_trained_count", default_var="0"))
        new_count  = labeled - last_count

        print(
            f"[train] Etiquetados en GOLD.TRAINING: {labeled:,}  "
            f"Último entrenamiento: {last_count:,}  "
            f"Nuevos: {new_count:,}  "
            f"Umbral: {RETRAIN_THRESHOLD:,}"
        )

        if new_count < RETRAIN_THRESHOLD:
            raise AirflowSkipException(
                f"Solo {new_count:,} pacientes nuevos etiquetados "
                f"(necesarios {RETRAIN_THRESHOLD:,}). Skip."
            )

        # ── Disparar entrenamiento ────────────────────────────────────────────
        print(f"[train] Disparando retrain con {labeled:,} pacientes etiquetados…")
        result = subprocess.run(
            [sys.executable, "/opt/spark-apps/training/train.py"],
            capture_output=True,
            text=True,
            timeout=1800,   # 30 min máx
        )
        if result.stdout:
            print(result.stdout.rstrip())
        if result.returncode != 0:
            raise RuntimeError(f"train.py falló:\n{result.stderr}")

        # Actualizar contador solo si entrenamiento fue exitoso
        Variable.set("last_trained_count", str(labeled))
        print(f"[train] last_trained_count actualizado → {labeled:,}")

    check_and_train()


train_challenger_dag()
