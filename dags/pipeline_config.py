"""
Archivo de constantes del pipeline — único punto de configuración.

Todos los parámetros se leen de variables de entorno (con valores por defecto),
por lo que basta con cambiarlos en docker-compose.yml (o en el entorno host)
sin tocar código.

Parámetros clave
----------------
PATIENTS_PER_MINUTE   : velocidad de llegada de datos al sistema (sim.)
TOTAL_PATIENTS        : total de pacientes a generar
INGEST_WINDOW_MINUTES : minutos simulados que procesa cada tick de Bronze
ASSEMBLY_WAIT_DAYS    : días pipeline antes de quarantina (anclado a sim_arrival_date)
RETRAIN_THRESHOLD     : nuevos pacientes etiquetados para disparar retrain
SCHEDULE_INGEST       : cron de los 5 DAGs de ingesta
SCHEDULE_REASSEMBLE   : cron del DAG de ensamblado + inferencia
SCHEDULE_TRAIN_CHECK  : cron del DAG de comprobación de retrain
"""
from __future__ import annotations
import os

# ── Generación de datos sintéticos ───────────────────────────────────────────
PATIENTS_PER_MINUTE   = int(os.getenv("PATIENTS_PER_MINUTE",   "10"))
TOTAL_PATIENTS        = int(os.getenv("TOTAL_PATIENTS",        "100000"))

# ── Bronze: ventana de tiempo simulado por tick ───────────────────────────────
# Con PATIENTS_PER_MINUTE=10 y INGEST_WINDOW_MINUTES=60:
#   600 pacientes por tick, ~167 ticks para 100k → ~5.5 h al ritmo del schedule
INGEST_WINDOW_MINUTES = int(os.getenv("INGEST_WINDOW_MINUTES", "60"))

# ── Gold Reassembler ──────────────────────────────────────────────────────────
# Días de pipeline (basados en sim_arrival_date) que esperamos a que un
# paciente complete todas las fuentes antes de mandarlo a cuarentena.
# Con datos en 7 pipeline-días, ASSEMBLY_WAIT_DAYS=3 da cuarentena desde día 4.
ASSEMBLY_WAIT_DAYS    = int(os.getenv("ASSEMBLY_WAIT_DAYS",    "3"))

# ── Entrenamiento champion-challenger ─────────────────────────────────────────
# Cuántos NUEVOS pacientes etiquetados en GOLD.TRAINING disparan un retrain.
RETRAIN_THRESHOLD     = int(os.getenv("RETRAIN_THRESHOLD",     "5000"))

# ── Schedules de los DAGs (expresiones cron) ─────────────────────────────────
SCHEDULE_INGEST      = os.getenv("SCHEDULE_INGEST",      "*/2 * * * *")
SCHEDULE_REASSEMBLE  = os.getenv("SCHEDULE_REASSEMBLE",  "*/10 * * * *")
SCHEDULE_TRAIN_CHECK = os.getenv("SCHEDULE_TRAIN_CHECK", "*/5 * * * *")
