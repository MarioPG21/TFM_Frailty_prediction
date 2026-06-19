"""
DAG de reloj de simulación — DESACTIVADO.

Sustituido por la lógica de ventana fija en los bronze ingestors.
Todos los datos están en la landing zone desde el inicio; los DAGs de
ingesta los procesan de forma incremental mediante watermarks.

Este archivo se mantiene como referencia histórica con schedule=None.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag


@dag(
    dag_id="clock",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["disabled"],
)
def clock_dag():
    pass


clock_dag()
