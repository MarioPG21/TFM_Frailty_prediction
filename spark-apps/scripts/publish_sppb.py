#!/usr/bin/env python3
"""
Publica oleadas de la Fuente B1 (encuestas SPPB, JSONL) al bucket landing de MinIO.

Uso:
    python spark-apps/scripts/publish_sppb.py --ticks 1
    python spark-apps/scripts/publish_sppb.py --ticks all --delay 0
"""
import argparse
import os
import sys
import time
from datetime import date
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
    parser = argparse.ArgumentParser(description="Publica Fuente B1 a MinIO landing")
    parser.add_argument("--ticks", default="1", help="Número de oleadas o 'all'")
    parser.add_argument("--delay", type=float, default=0, help="Segundos entre ticks")
    parser.add_argument("--day", default=None,
                        help="Día simulado YYYY-MM-DD: sube el mes que contiene ese día (idempotente)")
    args = parser.parse_args()

    bucket = os.getenv("MINIO_BUCKET_LANDING", "landing")
    source_dir = _SYNTHETIC / "source_b1" / "sppb_surveys"
    all_files = sorted(source_dir.glob("*.jsonl"))

    if not all_files:
        print(f"ERROR: No hay ficheros JSONL en {source_dir}", file=sys.stderr)
        sys.exit(1)

    client = _s3()

    if args.day:
        d = date.fromisoformat(args.day)
        stem = f"{d.year:04d}-{d.month:02d}"
        matches = [f for f in all_files if f.stem == stem]
        if not matches:
            print(f"[{stem}] sppb → sin datos para este mes")
            return
        fpath = matches[0]
        key = f"sppb/{d.year:04d}/{d.month:02d}/{fpath.name}"
        if _exists(client, bucket, key):
            print(f"[{stem}] sppb → ya publicado, omitiendo")
            return
        n_rows = sum(1 for _ in open(fpath, encoding="utf-8"))
        client.upload_file(str(fpath), bucket, key)
        print(f"[{stem}] sppb → s3://{bucket}/{key}  ({n_rows:,} registros)")
        return

    pending = []
    for fpath in all_files:
        year, month = fpath.stem.split("-")
        key = f"sppb/{year}/{month}/{fpath.name}"
        if not _exists(client, bucket, key):
            pending.append((fpath, key))

    to_publish = pending if args.ticks == "all" else pending[:int(args.ticks)]

    if not to_publish:
        print("Nada nuevo que publicar (todas las oleadas ya están en landing).")
        return

    for i, (fpath, key) in enumerate(to_publish):
        n_rows = sum(1 for _ in open(fpath, encoding="utf-8"))
        client.upload_file(str(fpath), bucket, key)
        print(f"[{fpath.stem}] sppb → s3://{bucket}/{key}  ({n_rows:,} registros)")
        if args.delay > 0 and i < len(to_publish) - 1:
            print(f"Esperando {args.delay:.0f} segundos antes del siguiente tick...")
            time.sleep(args.delay)


if __name__ == "__main__":
    main()
