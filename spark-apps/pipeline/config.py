import os

# ---------------------------------------------------------------------------
# Buckets — leídos de variables de entorno para que el mismo código funcione
# en local (MinIO) y en AWS (S3 nativo) sin modificaciones.
# ---------------------------------------------------------------------------
_LANDING = os.getenv("MINIO_BUCKET_LANDING", "landing")
_BRONZE  = os.getenv("MINIO_BUCKET_BRONZE",  "bronze")
_SILVER  = os.getenv("MINIO_BUCKET_SILVER",  "silver")
_GOLD    = os.getenv("MINIO_BUCKET_GOLD",     "gold")

def _s3(bucket: str, *parts: str) -> str:
    path = "/".join(parts)
    return f"s3a://{bucket}/{path}" if path else f"s3a://{bucket}"


# ---------------------------------------------------------------------------
# Rutas de capa Landing (destino de los scripts de publicación)
# ---------------------------------------------------------------------------
class LANDING:
    CLINICAL   = _s3(_LANDING, "clinical")
    SPPB       = _s3(_LANDING, "sppb")
    LIFESTYLE  = _s3(_LANDING, "lifestyle")
    GAIT       = _s3(_LANDING, "gait")
    LABELS     = _s3(_LANDING, "labels")


# ---------------------------------------------------------------------------
# Rutas de capa Bronze
# ---------------------------------------------------------------------------
class BRONZE:
    CLINICAL   = _s3(_BRONZE, "clinical")
    SPPB       = _s3(_BRONZE, "sppb")
    LIFESTYLE  = _s3(_BRONZE, "lifestyle")
    GAIT       = _s3(_BRONZE, "gait")
    LABELS     = _s3(_BRONZE, "labels")
    WATERMARKS = _s3(_BRONZE, "_control", "watermarks")


# ---------------------------------------------------------------------------
# Rutas de capa Silver (tablas limpias y cuarentenas)
# ---------------------------------------------------------------------------
class SILVER:
    CLINICAL             = _s3(_SILVER, "clinical")
    SPPB                 = _s3(_SILVER, "sppb")
    LIFESTYLE            = _s3(_SILVER, "lifestyle")
    GAIT                 = _s3(_SILVER, "gait")
    LABELS               = _s3(_SILVER, "labels")
    QUARANTINE_CLINICAL  = _s3(_SILVER, "quarantine_clinical")
    QUARANTINE_SPPB      = _s3(_SILVER, "quarantine_sppb")
    QUARANTINE_LIFESTYLE = _s3(_SILVER, "quarantine_lifestyle")
    QUARANTINE_GAIT      = _s3(_SILVER, "quarantine_gait")
    QUARANTINE_LABELS    = _s3(_SILVER, "quarantine_labels")


# ---------------------------------------------------------------------------
# Rutas de capa Gold
# ---------------------------------------------------------------------------
class GOLD:
    GAIT_FEATURES        = _s3(_GOLD, "gait_features")
    TRAINING             = _s3(_GOLD, "training")
    ASSEMBLED            = _s3(_GOLD, "assembled")
    INFERENCE_QUARANTINE = _s3(_GOLD, "inference_quarantine")
    PREDICTIONS          = _s3(_GOLD, "predictions")


# ---------------------------------------------------------------------------
# Claves de deduplicación por fuente (usadas en los MERGE de Bronze)
# ---------------------------------------------------------------------------
DEDUP_KEYS: dict[str, list[str]] = {
    "clinical":  ["patient_id", "snapshot_date"],
    "sppb":      ["response_id"],
    "lifestyle": ["response_id"],
    "gait":      ["event_id"],
    "labels":    ["patient_id", "snapshot_date"],
}

# ---------------------------------------------------------------------------
# Columna de watermark por fuente (pipeline-time; minute precision).
# Usada en Bronze para filtrar por ventana fija y avanzar el watermark.
# Separada de las fechas de evaluación (snapshot_date, label_available_date)
# que se usan para la lógica de negocio (splits temporales, anti-leakage).
# ---------------------------------------------------------------------------
DATE_COLS: dict[str, str] = {
    "clinical":  "updated_at",         # pipeline arrival ts (minute precision)
    "sppb":      "survey_date",        # idem (0-30 min lag vs clinical)
    "lifestyle": "survey_date",        # idem
    "gait":      "session_timestamp",  # idem
    "labels":    "updated_at",         # pipeline arrival ts (191-822 min lag)
}

# ---------------------------------------------------------------------------
# Ventana de ingesta Bronze (minutos simulados por tick).
# Configurable vía env var; valor por defecto 60.
# ---------------------------------------------------------------------------
import os as _os
INGEST_WINDOW_MINUTES: int      = int(_os.getenv("INGEST_WINDOW_MINUTES",      "60"))
# Gait tiene 20 strides/paciente (20× más filas que otras fuentes).
# Con ventana doble y 2 cores cubre el mismo pipeline-time en la mitad de ticks.
INGEST_WINDOW_MINUTES_GAIT: int = int(_os.getenv("INGEST_WINDOW_MINUTES_GAIT",
                                                   str(INGEST_WINDOW_MINUTES * 2)))

# ---------------------------------------------------------------------------
# Datos sintéticos — ruta base dentro de los contenedores
# ---------------------------------------------------------------------------
SYNTHETIC_DATA_PATH = "/opt/synthetic_data"
