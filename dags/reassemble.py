"""
DAG de reensamblado + inferencia (Gold).

Secuencia por tick:
  1. gold_gait_features  — agrega métricas de marcha desde Silver.GAIT
  2. gold_reassemble     — ensambla pacientes completos, quarantina incompletos
  3. gold_training       — reconstruye la training table (LEFT JOIN con labels)
  4. infer               — puntúa pacientes nuevos con el champion (si existe)

El paso de inferencia se salta sin error si no hay champion registrado en MLflow.

Configuración: dags/pipeline_config.py
"""
from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator

from pipeline_config import SCHEDULE_REASSEMBLE

_SUBMIT = (
    "$SPARK_HOME/bin/spark-submit"
    " --master spark://spark-master:7077"
    " --conf spark.cores.max=2"
    " --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
    " --conf spark.sql.catalog.spark_catalog="
    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    " --conf spark.driver.extraClassPath=/opt/spark/jars/*"
    " --conf spark.executor.extraClassPath=/opt/spark/jars/*"
    " /opt/spark-apps/scripts/run_layer.py"
)
_TIMEOUT = timedelta(minutes=45)


@dag(
    dag_id="reassemble",
    schedule=SCHEDULE_REASSEMBLE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "inference"],
)
def reassemble_dag():

    run_gait_features = BashOperator(
        task_id="run_gait_features",
        bash_command=_SUBMIT + " gold_gait_features",
        execution_timeout=_TIMEOUT,
    )

    run_reassemble = BashOperator(
        task_id="run_reassemble",
        bash_command=_SUBMIT + " gold_reassemble",
        execution_timeout=_TIMEOUT,
    )

    run_training_table = BashOperator(
        task_id="run_training_table",
        bash_command=_SUBMIT + " gold_training",
        execution_timeout=_TIMEOUT,
    )

    @task
    def run_inference() -> None:
        """Puntúa con el champion. Se salta sin error si no hay champion aún."""
        result = subprocess.run(
            [sys.executable, "/opt/spark-apps/training/infer.py"],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.stdout:
            print(result.stdout.rstrip())
        if result.returncode != 0:
            stderr = result.stderr or ""
            # Sin champion registrado todavía → skip silencioso
            if "RESOURCE_DOES_NOT_EXIST" in stderr or "champion" in stderr.lower():
                print("[infer] No hay champion registrado aún — se omite la inferencia.")
                return
            raise RuntimeError(f"infer.py falló:\n{stderr}")

    (
        run_gait_features
        >> run_reassemble
        >> run_training_table
        >> run_inference()
    )


reassemble_dag()
