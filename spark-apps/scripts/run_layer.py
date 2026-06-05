#!/usr/bin/env python3
"""
Ejecuta una capa del pipeline Medallion manualmente.

Uso (dentro del contenedor spark-master):
    python3.11 /opt/spark-apps/scripts/run_layer.py bronze
    python3.11 /opt/spark-apps/scripts/run_layer.py silver
    python3.11 /opt/spark-apps/scripts/run_layer.py gold

O vía docker exec desde el host:
    docker exec spark-master python3.11 /opt/spark-apps/scripts/run_layer.py bronze
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline.spark_session import get_spark


def run_bronze(spark):
    from pipeline.bronze.ingest_clinical import run as clinical
    from pipeline.bronze.ingest_gait import run as gait
    print("=== BRONZE ===")
    clinical(spark)
    gait(spark)


def run_silver(spark):
    from pipeline.silver.transform_clinical  import run as clinical
    from pipeline.silver.transform_sppb      import run as sppb
    from pipeline.silver.transform_lifestyle import run as lifestyle
    from pipeline.silver.transform_gait      import run as gait
    print("=== SILVER ===")
    clinical(spark)
    sppb(spark)
    lifestyle(spark)
    gait(spark)


def run_gold(spark):
    from pipeline.gold.gait_features  import run as gait_feat
    from pipeline.gold.training_table import run as training
    print("=== GOLD ===")
    gait_feat(spark)
    training(spark)


_LAYERS = {
    "bronze": run_bronze,
    "silver": run_silver,
    "gold":   run_gold,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in _LAYERS:
        print(f"Uso: run_layer.py {'|'.join(_LAYERS)}", file=sys.stderr)
        sys.exit(1)

    layer = sys.argv[1]
    spark = get_spark(f"run-{layer}")
    try:
        _LAYERS[layer](spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
