#!/usr/bin/env python3
"""
Elimina las tablas Bronze y watermarks de las fuentes indicadas para
permitir re-ingesta desde cero con el nuevo esquema (sim_arrival_date).

Uso:
    python3.11 reset_sources.py sppb lifestyle labels
    python3.11 reset_sources.py --all
"""
import argparse
import os
import sys

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


BUCKET    = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
KEY_ID    = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET    = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")

ALL_SOURCES = ["clinical", "gait", "sppb", "lifestyle", "labels"]


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=KEY_ID,
        aws_secret_access_key=SECRET,
    )


def delete_prefix(client, bucket, prefix):
    """Borra recursivamente todos los objetos bajo prefix."""
    paginator = client.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])
        if not objects:
            continue
        client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
        )
        deleted += len(objects)
    return deleted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("sources", nargs="*", choices=ALL_SOURCES + ["all"])
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    sources = ALL_SOURCES if (args.all or "all" in args.sources) else args.sources
    if not sources:
        parser.print_help()
        sys.exit(1)

    client = _s3()

    for src in sources:
        prefix = f"bronze/{src}/"
        n = delete_prefix(client, BUCKET, prefix)
        print(f"[{src}] bronze/{src}/  — {n} objetos eliminados")

    # Reset watermarks tabla (elimina filas de las fuentes seleccionadas).
    # La tabla watermarks también está en MinIO como Delta; borrando los
    # parquets la forzamos a recrearse en el próximo run.
    wm_prefix = "bronze/_control/"
    n = delete_prefix(client, BUCKET, wm_prefix)
    print(f"[watermarks] bronze/_control/  — {n} objetos eliminados")

    print("\nReset completado. Re-ingestar con --sim-day para repoblar.")


if __name__ == "__main__":
    main()
