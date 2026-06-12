"""
DAG de reloj de simulación (clock).

Avanza el día simulado guardado en la Variable de Airflow `sim_day`
(formato YYYY-MM-DD) y lanza el pipeline de transformación.

Flujo por tick:
  1. advance_clock  — lee sim_day, calcula el día siguiente.
  2. publish_sources — sube la oleada de cada fuente al landing de MinIO.
     Cada script de publicación es idempotente: si el fichero del mes ya está
     en MinIO lo omite; solo sube si es el primer día de ese mes simulado.
  3. confirm_clock  — escribe el nuevo día en la Variable (solo tras éxito).
  4. trigger_pipeline — dispara el DAG `pipeline` y espera su finalización.

Atomicidad: confirm_clock solo corre si publish_sources tiene éxito.
Si el pipeline falla, el reloj ya está confirmado; la siguiente ejecución del
reloj avanza un día más, pero el pipeline recoge todo lo acumulado gracias
al watermark incremental del Bronze.

Variable de Airflow requerida:
  sim_day (YYYY-MM-DD) — si no existe se usa 2023-12-31 (día anterior a la
  cohorte) para que el primer tick publique 2024-01-01.

Schedule: None (disparo manual). Cambiar a "*/5 * * * *" para auto-avance.
"""
from __future__ import annotations

import subprocess
import sys
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

COHORT_START = date(2024, 1, 1)
COHORT_END   = date(2025, 6, 30)
_BEFORE_START = (COHORT_START - timedelta(days=1)).isoformat()  # 2023-12-31

PUBLISH_SCRIPTS = [
    "/opt/spark-apps/scripts/publish_clinical.py",
    "/opt/spark-apps/scripts/publish_sppb.py",
    "/opt/spark-apps/scripts/publish_lifestyle.py",
    "/opt/spark-apps/scripts/publish_gait.py",
    "/opt/spark-apps/scripts/publish_labels.py",
]


@dag(
    dag_id="clock",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["orchestration", "simulation"],
    doc_md=__doc__,
)
def clock_dag():

    @task
    def advance_clock() -> str:
        """Calcula el siguiente día simulado sin confirmar todavía el avance."""
        sim_day_str = Variable.get("sim_day", default_var=_BEFORE_START)
        sim_day = date.fromisoformat(sim_day_str)
        if sim_day >= COHORT_END:
            raise AirflowSkipException(
                f"Cohorte completada: {sim_day} >= {COHORT_END}"
            )
        next_day = sim_day + timedelta(days=1)
        print(f"Reloj: {sim_day} → {next_day}")
        return next_day.isoformat()

    @task
    def publish_sources(next_day: str) -> None:
        """Sube la oleada del día simulado a landing para todas las fuentes."""
        for script in PUBLISH_SCRIPTS:
            result = subprocess.run(
                [sys.executable, script, "--day", next_day],
                capture_output=True,
                text=True,
                timeout=120,
            )
            if result.stdout:
                print(result.stdout.rstrip())
            if result.returncode != 0:
                raise RuntimeError(
                    f"Publicación fallida [{script}]:\n{result.stderr}"
                )

    @task
    def confirm_clock(next_day: str) -> None:
        """Consolida el avance: escribe el nuevo día en la Variable de Airflow."""
        Variable.set("sim_day", next_day)
        print(f"sim_day confirmado: {next_day}")

    trigger_pipeline = TriggerDagRunOperator(
        task_id="trigger_pipeline",
        trigger_dag_id="pipeline",
        wait_for_completion=False,
    )

    next_day = advance_clock()
    pub = publish_sources(next_day)
    conf = confirm_clock(next_day)
    pub >> conf >> trigger_pipeline


clock_dag()
