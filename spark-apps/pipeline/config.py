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
    GAIT_FEATURES = _s3(_GOLD, "gait_features")
    TRAINING      = _s3(_GOLD, "training")


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
# Columna de fecha principal por fuente (usada para el watermark y el
# particionado por year/month)
# ---------------------------------------------------------------------------
DATE_COLS: dict[str, str] = {
    "clinical":  "snapshot_date",
    "sppb":      "survey_date",
    "lifestyle": "survey_date",
    "gait":      "session_timestamp",
    # Labels use label_available_date: governs when the record "arrives" (anti-leakage)
    "labels":    "label_available_date",
}

# ---------------------------------------------------------------------------
# Datos sintéticos — ruta base dentro de los contenedores
# ---------------------------------------------------------------------------
SYNTHETIC_DATA_PATH = "/opt/synthetic_data"
