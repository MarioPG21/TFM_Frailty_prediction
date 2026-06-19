#!/usr/bin/env python3
"""
Sube TODOS los datos sintéticos a la landing zone de MinIO.

Ejecutar UNA SOLA VEZ antes de arrancar el pipeline.
Desde el host (con MinIO en localhost:9000):

    python spark-apps/scripts/upload_to_landing.py

Desde dentro del contenedor Airflow:

    python /opt/spark-apps/scripts/upload_to_landing.py

Mapeo local → landing:
  synthetic_data/source_a/clinical_records/  → landing/clinical/
  synthetic_data/source_b1/sppb_surveys/     → landing/sppb/
  synthetic_data/source_b2/lifestyle_surveys/→ landing/lifestyle/
  synthetic_data/source_c/gait_events/       → landing/gait/
  synthetic_data/labels/                     → landing/labels/
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# ── Config ────────────────────────────────────────────────────────────────────
ENDPOINT  = os.getenv("MINIO_ENDPOINT",       "http://localhost:9000")
KEY       = os.getenv("AWS_ACCESS_KEY_ID",    "minioadmin")
SECRET    = os.getenv("AWS_SECRET_ACCESS_KEY","minioadmin123")
BUCKET    = os.getenv("MINIO_BUCKET_LANDING", "landing")

# Raíz de los datos sintéticos (relativa al directorio del proyecto)
_SCRIPT_DIR  = Path(__file__).resolve().parent
_PROJECT_DIR = _SCRIPT_DIR.parent.parent          # …/TFM_Frailty_prediction
DATA_DIR     = _PROJECT_DIR / "synthetic_data"

# Mapeo: subcarpeta local → prefijo en la landing zone
SOURCES: dict[str, str] = {
    "source_a/clinical_records":   "clinical",
    "source_b1/sppb_surveys":      "sppb",
    "source_b2/lifestyle_surveys": "lifestyle",
    "source_c/gait_events":        "gait",
    "labels":                      "labels",
}


def _ensure_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=BUCKET)
        print(f"  Bucket '{BUCKET}' creado.")


def main() -> None:
    if not DATA_DIR.exists():
        print(
            f"ERROR: No se encuentra {DATA_DIR}\n"
            f"Ejecuta primero:  python generate_frailty.py",
            file=sys.stderr,
        )
        sys.exit(1)

    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    _ensure_bucket(s3)

    total = 0
    for subdir, prefix in SOURCES.items():
        src = DATA_DIR / subdir
        if not src.exists():
            print(f"  AVISO: {src} no existe — saltando.")
            continue
        files = sorted(src.glob("*"))
        if not files:
            print(f"  AVISO: {src} está vacío — saltando.")
            continue
        print(f"\n  [{prefix}] {len(files)} ficheros desde {src.name}/")
        for f in files:
            if f.is_dir():
                continue
            key = f"{prefix}/{f.name}"
            s3.upload_file(str(f), BUCKET, key)
            print(f"    ✓ {key}  ({f.stat().st_size // 1024:,} KB)")
            total += 1

    print(f"\n{'='*60}")
    print(f"  COMPLETADO: {total} ficheros subidos a s3://{BUCKET}/")
    print(f"  Endpoint: {ENDPOINT}")
    print(f"{'='*60}\n")
    print("  Ahora puedes arrancar los DAGs de Airflow.")
    print("  Los ingestores recogerán los datos automáticamente")
    print(f"  en ventanas de $INGEST_WINDOW_MINUTES minutos simulados.")


if __name__ == "__main__":
    main()
