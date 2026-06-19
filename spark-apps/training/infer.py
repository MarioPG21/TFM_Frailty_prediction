#!/usr/bin/env python3
"""
Inferencia con el champion actual del Model Registry.

Carga GOLD.ASSEMBLED, descarta los pacientes ya puntuados (idempotencia),
aplica el champion y escribe las predicciones en GOLD.PREDICTIONS.

Verificaciones explícitas:
  - Ninguna columna de NON_FEATURE_COLS entra al modelo.
  - Se imprime la lista exacta de features usadas.

Variables de entorno requeridas:
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
  MINIO_ENDPOINT      — http://minio:9000 (Docker) / http://localhost:9000 (host)
  MLFLOW_TRACKING_URI — http://mlflow:5000 (Docker) / http://localhost:5000 (host)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import mlflow
import mlflow.sklearn
import pandas as pd
from deltalake import DeltaTable, write_deltalake

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_NAME = "frailty-classifier"

# Mismas columnas excluidas que en train.py — fuente de verdad: NON_FEATURE_COLS
NON_FEATURE_COLS: set[str] = {
    "patient_id", "snapshot_date", "year", "month",
    "ingestion_timestamp", "source_file", "updated_at",
    "label_available_date", "frailty_label",
    "session_id", "session_date",
    "sim_arrival_date",       # metadato de orquestación
    "assembly_timestamp",     # metadato de ensamblado
}


def _storage_options() -> dict:
    return {
        "AWS_ENDPOINT_URL":                os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        "AWS_ACCESS_KEY_ID":               os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY":           os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_ALLOW_HTTP":                  "true",
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
    }


def _gold_path(table: str) -> str:
    bucket = os.environ.get("MINIO_BUCKET_GOLD", "gold")
    return f"s3://{bucket}/{table}"


def load_assembled() -> pd.DataFrame:
    path = _gold_path("assembled")
    log.info("Leyendo GOLD.ASSEMBLED: %s", path)
    dt = DeltaTable(path, storage_options=_storage_options())
    df = dt.to_pandas()
    log.info("GOLD.ASSEMBLED: %d pacientes ensamblados", len(df))
    return df


def already_scored_ids() -> set[str]:
    path = _gold_path("predictions")
    try:
        dt = DeltaTable(path, storage_options=_storage_options())
        ids = set(dt.to_pandas()["patient_id"].tolist())
        log.info("Ya puntuados previamente: %d pacientes", len(ids))
        return ids
    except Exception:
        return set()


def main() -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    # Cargar champion
    model_uri = f"models:/{MODEL_NAME}@champion"
    log.info("Cargando champion: %s", model_uri)
    model = mlflow.sklearn.load_model(model_uri)

    # Obtener versión del champion para trazabilidad
    client = mlflow.MlflowClient()
    champion_ver = client.get_model_version_by_alias(MODEL_NAME, "champion")
    log.info("Champion: v%s  run_id=%s", champion_ver.version, champion_ver.run_id)

    # Cargar ensamblados y filtrar ya puntuados
    assembled = load_assembled()
    scored = already_scored_ids()
    new_df = assembled[~assembled["patient_id"].isin(scored)].copy()
    log.info("Pacientes nuevos a puntuar: %d  (ya puntuados excluidos: %d)",
             len(new_df), len(scored))

    if len(new_df) == 0:
        log.info("Sin pacientes nuevos. Nada que hacer.")
        return

    # Construir matriz de features — verificación explícita de exclusión
    feature_cols = [c for c in new_df.columns if c not in NON_FEATURE_COLS]
    excluded = [c for c in new_df.columns if c in NON_FEATURE_COLS]
    log.info("Features usadas (%d): %s", len(feature_cols), feature_cols)
    log.info("Columnas excluidas  (%d): %s", len(excluded), excluded)

    # Verificar que sim_arrival_date y label_available_date no están en features
    for forbidden in ("sim_arrival_date", "label_available_date", "frailty_label"):
        assert forbidden not in feature_cols, \
            f"¡LEAKAGE DETECTADO! '{forbidden}' está en feature_cols"

    X = new_df[feature_cols]
    probs = model.predict_proba(X)[:, 1]

    # Umbral fijo 0.5 para inferencia en producción
    # (el umbral óptimo de val se usó para evaluar el champion; en producción
    #  se usa 0.5 salvo que se pase explícitamente)
    threshold = float(os.environ.get("INFERENCE_THRESHOLD", "0.5"))
    preds = (probs >= threshold).astype(int)

    now = datetime.now(timezone.utc).isoformat()
    results = pd.DataFrame({
        "patient_id":       new_df["patient_id"].values,
        "frailty_prob":     probs.round(4),
        "frailty_pred":     preds,
        "threshold":        threshold,
        "champion_version": int(champion_ver.version),
        "scored_at":        now,
    })

    # ── Estadísticas ──────────────────────────────────────────────────────────
    n = len(results)
    n_pos = int(preds.sum())
    n_neg = n - n_pos
    print()
    print("=" * 60)
    print(f"INFERENCIA COMPLETADA — {n} pacientes nuevos")
    print("=" * 60)
    print(f"  Champion v{champion_ver.version}")
    print(f"  Features usadas          : {len(feature_cols)}")
    print(f"  sim_arrival_date en X    : {'sim_arrival_date' in feature_cols}  ← debe ser False")
    print(f"  label_available_date en X: {'label_available_date' in feature_cols}  ← debe ser False")
    print(f"  Umbral de decisión       : {threshold}")
    print(f"  Frágil predicho          : {n_pos:,} ({100*n_pos/n:.1f}%)")
    print(f"  No frágil predicho       : {n_neg:,} ({100*n_neg/n:.1f}%)")
    print(f"  Prob media               : {probs.mean():.4f}")
    print(f"  Prob mín / máx           : {probs.min():.4f} / {probs.max():.4f}")
    print()
    print("Muestra (20 primeros):")
    print(results[["patient_id", "frailty_prob", "frailty_pred"]].head(20).to_string(index=False))
    print()

    # ── Guardar predicciones ──────────────────────────────────────────────────
    pred_path = _gold_path("predictions")
    write_deltalake(
        pred_path,
        results,
        storage_options=_storage_options(),
        mode="append",
        schema_mode="merge",
    )
    log.info("Predicciones guardadas en %s  (%d nuevas filas)", pred_path, n)


if __name__ == "__main__":
    main()
