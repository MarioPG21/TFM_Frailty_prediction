"""
DAG de ingesta de telemetría de marcha (Fuente C).

Configuración: dags/pipeline_config.py
"""
from __future__ import annotations

import os
import shlex
import subprocess
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.bash import BashOperator

from pipeline_config import SCHEDULE_INGEST

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
_TIMEOUT = timedelta(minutes=30)


def _run(command: str) -> None:
    cmd = _SUBMIT.replace("$SPARK_HOME", os.environ.get("SPARK_HOME", "/opt/spark"))
    result = subprocess.run(
        shlex.split(cmd) + [command],
        capture_output=True, text=True, timeout=int(_TIMEOUT.total_seconds()),
    )
    if result.stdout:
        print(result.stdout.rstrip())
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr.rstrip())
        raise RuntimeError(f"{command} falló (rc={result.returncode})")
    if any(m in result.stdout for m in ("Ventana sin datos", "Landing vacío", "No se pudo leer")):
        raise AirflowSkipException("Sin datos nuevos en este tick.")


@dag(
    dag_id="ingest_gait",
    schedule=SCHEDULE_INGEST,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "gait"],
)
def ingest_gait_dag():

    @task
    def run_bronze():
        _run("bronze_gait")

    run_silver = BashOperator(
        task_id="run_silver",
        bash_command=_SUBMIT + " silver_gait",
        execution_timeout=_TIMEOUT,
    )

    run_bronze() >> run_silver


ingest_gait_dag()
