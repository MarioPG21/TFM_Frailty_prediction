#!/usr/bin/env python3
"""
Publica el flujo de confirmación clínica diferida (labels CSV) al bucket landing de MinIO.

Uso (desde la raíz del proyecto):
    python spark-apps/scripts/publish_labels.py --ticks 1
    python spark-apps/scripts/publish_labels.py --ticks all
"""
import argparse
import os
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent
_SYNTHETIC = Path(os.getenv("SYNTHETIC_DATA_PATH",
                             str(_PROJECT_ROOT / "synthetic_data")))
if not _SYNTHETIC.exists():
    _SYNTHETIC = Path("/opt/synthetic_data")


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    )


def _exists(client, bucket, key):
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def main():
    parser = argparse.ArgumentParser(description="Publica flujo labels a MinIO landing")
    parser.add_argument("--ticks", default="1", help="Número de periodos o 'all'")
    parser.add_argument("--delay", type=float, default=0, help="Segundos entre ticks")
    args = parser.parse_args()

    bucket = os.getenv("MINIO_BUCKET_LANDING", "landing")
    source_dir = _SYNTHETIC / "labels"
    all_files = sorted(source_dir.glob("*.csv"))

    if not all_files:
        print(f"ERROR: No hay ficheros CSV en {source_dir}", file=sys.stderr)
        sys.exit(1)

    client = _s3()

    pending = []
    for fpath in all_files:
        year, month = fpath.stem.split("-")
        key = f"labels/{year}/{month}/{fpath.name}"
        if not _exists(client, bucket, key):
            pending.append((fpath, key))

    to_publish = pending if args.ticks == "all" else pending[:int(args.ticks)]

    if not to_publish:
        print("Nada nuevo que publicar (todos los periodos ya están en landing).")
        return

    for i, (fpath, key) in enumerate(to_publish):
        n_rows = sum(1 for _ in open(fpath, encoding="utf-8")) - 1
        client.upload_file(str(fpath), bucket, key)
        print(f"[{fpath.stem}] labels → s3://{bucket}/{key}  ({n_rows:,} etiquetas)")
        if args.delay > 0 and i < len(to_publish) - 1:
            print(f"Esperando {args.delay:.0f} segundos antes del siguiente tick...")
            time.sleep(args.delay)


if __name__ == "__main__":
    main()
