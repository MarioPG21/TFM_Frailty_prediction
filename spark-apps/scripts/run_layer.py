#!/usr/bin/env python3
"""
Ejecuta una capa del pipeline Medallion manualmente.

Uso (dentro del contenedor spark-master o vía spark-submit):

  Capas completas:
    run_layer.py bronze
    run_layer.py silver
    run_layer.py gold

  Por fuente (usado por los DAGs de ingesta multicadencia):
    run_layer.py bronze_clinical
    run_layer.py bronze_gait
    run_layer.py bronze_sppb
    run_layer.py bronze_lifestyle
    run_layer.py bronze_labels
    run_layer.py silver_clinical
    run_layer.py silver_gait
    run_layer.py silver_sppb
    run_layer.py silver_lifestyle
    run_layer.py silver_labels
    run_layer.py gold_gait_features
    run_layer.py gold_reassemble
    run_layer.py gold_training
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline.spark_session import get_spark


# ---------------------------------------------------------------------------
# Capas completas
# ---------------------------------------------------------------------------

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
    from pipeline.silver.transform_labels    import run as labels
    print("=== SILVER ===")
    clinical(spark)
    sppb(spark)
    lifestyle(spark)
    gait(spark)
    labels(spark)


def run_gold(spark):
    from pipeline.gold.gait_features  import run as gait_feat
    from pipeline.gold.training_table import run as training
    from pipeline.gold.reassembler    import run as reassemble
    print("=== GOLD ===")
    gait_feat(spark)
    reassemble(spark)
    training(spark)


# ---------------------------------------------------------------------------
# Por fuente — Bronze
# ---------------------------------------------------------------------------

def run_bronze_clinical(spark):
    from pipeline.bronze.ingest_clinical import run_clinical
    print("=== BRONZE clinical ===")
    run_clinical(spark)


def run_bronze_gait(spark):
    from pipeline.bronze.ingest_gait import run
    print("=== BRONZE gait ===")
    run(spark)


def run_bronze_sppb(spark):
    from pipeline.bronze.ingest_clinical import run_sppb
    print("=== BRONZE sppb ===")
    run_sppb(spark)


def run_bronze_lifestyle(spark):
    from pipeline.bronze.ingest_clinical import run_lifestyle
    print("=== BRONZE lifestyle ===")
    run_lifestyle(spark)


def run_bronze_labels(spark):
    from pipeline.bronze.ingest_clinical import run_labels
    print("=== BRONZE labels ===")
    run_labels(spark)


# ---------------------------------------------------------------------------
# Por fuente — Silver
# ---------------------------------------------------------------------------

def run_silver_clinical(spark):
    from pipeline.silver.transform_clinical import run
    print("=== SILVER clinical ===")
    run(spark)


def run_silver_gait(spark):
    from pipeline.silver.transform_gait import run
    print("=== SILVER gait ===")
    run(spark)


def run_silver_sppb(spark):
    from pipeline.silver.transform_sppb import run
    print("=== SILVER sppb ===")
    run(spark)


def run_silver_lifestyle(spark):
    from pipeline.silver.transform_lifestyle import run
    print("=== SILVER lifestyle ===")
    run(spark)


def run_silver_labels(spark):
    from pipeline.silver.transform_labels import run
    print("=== SILVER labels ===")
    run(spark)


# ---------------------------------------------------------------------------
# Gold individual
# ---------------------------------------------------------------------------

def run_gold_gait_features(spark):
    from pipeline.gold.gait_features import run
    print("=== GOLD gait_features ===")
    run(spark)


def run_gold_reassemble(spark):
    from pipeline.gold.reassembler import run
    print("=== GOLD reassemble ===")
    run(spark)


def run_gold_training(spark):
    from pipeline.gold.training_table import run
    print("=== GOLD training_table ===")
    run(spark)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

_COMMANDS = {
    # capas completas
    "bronze":              run_bronze,
    "silver":              run_silver,
    "gold":                run_gold,
    # bronze por fuente
    "bronze_clinical":     run_bronze_clinical,
    "bronze_gait":         run_bronze_gait,
    "bronze_sppb":         run_bronze_sppb,
    "bronze_lifestyle":    run_bronze_lifestyle,
    "bronze_labels":       run_bronze_labels,
    # silver por fuente
    "silver_clinical":     run_silver_clinical,
    "silver_gait":         run_silver_gait,
    "silver_sppb":         run_silver_sppb,
    "silver_lifestyle":    run_silver_lifestyle,
    "silver_labels":       run_silver_labels,
    # gold individual
    "gold_gait_features":  run_gold_gait_features,
    "gold_reassemble":     run_gold_reassemble,
    "gold_training":       run_gold_training,
}


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Ejecuta una capa o fuente del pipeline Medallion.",
    )
    parser.add_argument(
        "command",
        choices=list(_COMMANDS),
        metavar="COMANDO",
        help=f"Uno de: {', '.join(_COMMANDS)}",
    )
    args = parser.parse_args()

    spark = get_spark(f"run-{args.command}")
    try:
        _COMMANDS[args.command](spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
