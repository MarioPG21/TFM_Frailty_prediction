"""
DAG de pipeline medallion: Bronze → Silver → Gold.

Se dispara desde el DAG `clock` (o manualmente para debugging).
No tiene schedule propio: schedule=None.

Cada capa se ejecuta vía BashOperator que llama directamente a spark-submit
contra el cluster spark://spark-master:7077. El script `run_layer.py` gestiona:
  - Watermarks incrementales por fuente (Bronze skip si sin datos nuevos).
  - MERGE idempotente en Delta Lake (Silver/Gold).
  - Anti-leakage as-of join en Gold (label_available_date <= snapshot_date).

El classpath de Delta Lake y S3A ya está en /opt/spark/jars/ en todos los
contenedores (Airflow y Spark workers), montado desde la imagen Docker.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator

_SUBMIT = (
    "$SPARK_HOME/bin/spark-submit"
    " --master spark://spark-master:7077"
    " --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
    " --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    " --conf spark.driver.extraClassPath=/opt/spark/jars/*"
    " --conf spark.executor.extraClassPath=/opt/spark/jars/*"
    " /opt/spark-apps/scripts/run_layer.py"
)

_TIMEOUT = timedelta(minutes=30)


@dag(
    dag_id="pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline", "medallion"],
    doc_md=__doc__,
)
def pipeline_dag():

    bronze = BashOperator(
        task_id="bronze",
        bash_command=f"{_SUBMIT} bronze",
        execution_timeout=_TIMEOUT,
    )

    silver = BashOperator(
        task_id="silver",
        bash_command=f"{_SUBMIT} silver",
        execution_timeout=_TIMEOUT,
    )

    gold = BashOperator(
        task_id="gold",
        bash_command=f"{_SUBMIT} gold",
        execution_timeout=_TIMEOUT,
    )

    bronze >> silver >> gold


pipeline_dag()
