#!/usr/bin/env python3
"""
Fase 4 — Entrenamiento, validación y sistema champion-challenger.

Partición temporal dinámica por percentil (sin aleatoriedad):
  Train:      snapshot_date ≤ P70 de las fechas con etiqueta  (~70% de pacientes)
  Validation: P70 < snapshot_date ≤ P90                       (~20%)
  Test:       snapshot_date > P90                             (~10%)

Los cortes se calculan en tiempo de ejecución a partir de los datos presentes
en la tabla Gold, por lo que el split se adapta automáticamente a medida que
llegan nuevos lotes (Fase 5). Los cortes reales se registran como parámetros
en MLflow para trazabilidad completa.

Flujo:
  1. Leer tabla Gold de entrenamiento desde Delta Lake (MinIO).
  2. Excluir filas sin etiqueta; registrar su volumen.
  3. Partir temporalmente en train / val / test.
  4. Grid search sobre regularización de regresión logística.
     MLflow: un run padre + un run hijo anidado por punto de la rejilla.
  5. Mejor candidato (máximo AUC-PR en val) registrado como 'challenger' y evaluado en test.
  6. Champion-challenger:
       · Sin champion previo y supera umbrales → champion (retrain en todos los datos).
       · Challenger gana a champion → nuevo champion, anterior → retired, challenger eliminado.
       · Challenger pierde o no supera umbrales → challenger → rejected.
  7. El modelo champion se reentrena sobre todos los datos antes de registrarse.

Variables de entorno requeridas:
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  — credenciales MinIO
  MINIO_ENDPOINT       — http://minio:9000  (desde Docker)
                         http://localhost:9000 (desde host)
  MLFLOW_TRACKING_URI  — http://mlflow:5000  (desde Docker)
                         http://localhost:5000 (desde host)
  MINIO_BUCKET_GOLD    — nombre del bucket gold (default: gold)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from deltalake import DeltaTable
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder, StandardScaler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
TRAINING_EXPERIMENT = os.environ.get("MLFLOW_TRAINING_EXPERIMENT", "frailty-training")
MODEL_NAME          = "frailty-classifier"
RANDOM_SEED         = 42

# Percentiles para el split temporal dinámico.
# Calculados sobre la distribución de snapshot_date en los datos con etiqueta.
TRAIN_PERCENTILE = 0.70   # todo hasta el 70% de la distribución de fechas → train
VAL_PERCENTILE   = 0.90   # 70–90% → val;  >90% → test

# Umbrales mínimos de calidad para promover a champion
MIN_AUC_PR  = 0.35   # > prevalencia (~0.25): un clasificador trivial no pasa
MIN_AUC_ROC = 0.65

# Rejilla de hiperparámetros (C × penalty → 8 combinaciones)
PARAM_GRID: list[dict] = [
    {"C": c, "penalty": p}
    for c in [0.01, 0.1, 1.0, 10.0]
    for p in ["l2", "l1"]
]

# Columnas que NO son características (identificadores, fechas, auditoría, target)
NON_FEATURE_COLS: set[str] = {
    "patient_id",
    "snapshot_date",
    "year",
    "month",
    "ingestion_timestamp",
    "source_file",
    "updated_at",
    "label_available_date",   # marcador anti-leakage: participó en el join Gold, no es feature
    "frailty_label",          # target
    "session_id",             # id de sesión de marcha (excluida en Gold, por si acaso)
    "session_date",           # fecha de sesión (excluida en Gold, por si acaso)
    "sim_arrival_date",       # metadato de orquestación: cuándo llegó el lote; no es feature clínica
}
TARGET_COL = "frailty_label"


def _gold_path() -> str:
    bucket = os.environ.get("MINIO_BUCKET_GOLD", "gold")
    return f"s3://{bucket}/training"


def _storage_options() -> dict:
    return {
        "AWS_ENDPOINT_URL":                os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        "AWS_ACCESS_KEY_ID":               os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY":           os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_ALLOW_HTTP":                  "true",
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
    }


# ---------------------------------------------------------------------------
# 1. Carga de datos
# ---------------------------------------------------------------------------
def load_gold_table() -> tuple[pd.DataFrame, int]:
    """
    Carga la tabla Gold de entrenamiento desde Delta Lake (MinIO).
    Devuelve (DataFrame, versión_delta).
    """
    path = _gold_path()
    log.info("Leyendo tabla Gold: %s", path)
    dt = DeltaTable(path, storage_options=_storage_options())
    delta_version = dt.version()
    df = dt.to_pandas()
    log.info(
        "Cargadas %d filas × %d columnas  (versión Delta: %d)",
        len(df), len(df.columns), delta_version,
    )
    return df, delta_version


# ---------------------------------------------------------------------------
# 2. Partición temporal
# ---------------------------------------------------------------------------
def _normalize_date_col(series: pd.Series) -> pd.Series:
    """Normaliza snapshot_date a objetos datetime.date independientemente del dtype."""
    if pd.api.types.is_datetime64_any_dtype(series):
        return series.dt.date
    sample = series.dropna().iloc[0] if len(series.dropna()) > 0 else None
    if sample is None:
        return series
    if isinstance(sample, str):
        return pd.to_datetime(series).dt.date
    return series  # ya son datetime.date


def split_temporal(df: pd.DataFrame) -> tuple:
    """
    Parte el DataFrame en train / val / test de forma estrictamente temporal
    usando cortes por percentil calculados sobre los datos presentes.

    Percentiles: P70 separa train/val; P90 separa val/test.
    Los cortes reales (fechas) se devuelven junto con los splits para
    registrarlos en MLflow.

    Returns
    -------
    X_train, y_train, X_val, y_val, X_test, y_test, feature_cols,
    n_unlabeled, date_train_end, date_val_end
    """
    df = df.copy()
    df["snapshot_date"] = _normalize_date_col(df["snapshot_date"])

    # Excluir filas sin etiqueta
    labeled = df[df[TARGET_COL].notna()].copy()
    n_unlabeled = len(df) - len(labeled)
    log.info(
        "Filas con etiqueta: %d  |  sin etiqueta (excluidas): %d",
        len(labeled), n_unlabeled,
    )

    # Cortes dinámicos por percentil sobre la distribución de fechas
    dates_numeric = pd.to_datetime(labeled["snapshot_date"]).astype("int64")
    p70_ts = dates_numeric.quantile(TRAIN_PERCENTILE)
    p90_ts = dates_numeric.quantile(VAL_PERCENTILE)
    date_train_end = pd.Timestamp(p70_ts).date()
    date_val_end   = pd.Timestamp(p90_ts).date()

    log.info(
        "Cortes dinámicos: train ≤ %s  |  val ≤ %s  |  test > %s",
        date_train_end, date_val_end, date_val_end,
    )

    train_mask = labeled["snapshot_date"] <= date_train_end
    val_mask   = (labeled["snapshot_date"] > date_train_end) & \
                 (labeled["snapshot_date"] <= date_val_end)
    test_mask  = labeled["snapshot_date"] > date_val_end

    train = labeled[train_mask]
    val   = labeled[val_mask]
    test  = labeled[test_mask]

    log.info("Split  train=%d  val=%d  test=%d", len(train), len(val), len(test))
    for name, split in [("train", train), ("val", val), ("test", test)]:
        if len(split) > 0:
            prev = split[TARGET_COL].mean()
            log.info("  %-5s  prevalencia frailty=%.1f%%  fechas [%s … %s]",
                     name, prev * 100,
                     split["snapshot_date"].min(), split["snapshot_date"].max())

    feature_cols = [c for c in labeled.columns if c not in NON_FEATURE_COLS]
    log.info("Características (%d): %s", len(feature_cols), feature_cols)

    def _xy(split: pd.DataFrame):
        return split[feature_cols].copy(), split[TARGET_COL].astype(int)

    X_tr, y_tr = _xy(train)
    X_va, y_va = _xy(val)
    X_te, y_te = _xy(test)

    return X_tr, y_tr, X_va, y_va, X_te, y_te, feature_cols, n_unlabeled, date_train_end, date_val_end


# ---------------------------------------------------------------------------
# 3. Construcción del Pipeline sklearn
# ---------------------------------------------------------------------------
def build_pipeline(feature_cols: list[str], C: float = 1.0, penalty: str = "l2") -> Pipeline:
    """
    Pipeline de preprocesado + regresión logística.

    - sex (M/F): imputación con moda + OrdinalEncoder.
    - numéricas:  imputación con mediana + StandardScaler.
    - class_weight="balanced": compensa el desbalanceo sin muestreo.
    - solver="liblinear": soporta tanto l1 como l2.
    """
    sex_cols     = [c for c in feature_cols if c == "sex"]
    numeric_cols = [c for c in feature_cols if c != "sex"]

    transformers = []
    if sex_cols:
        transformers.append((
            "sex",
            Pipeline([
                ("imp", SimpleImputer(strategy="most_frequent")),
                ("enc", OrdinalEncoder(
                    handle_unknown="use_encoded_value", unknown_value=-1,
                )),
            ]),
            sex_cols,
        ))
    if numeric_cols:
        transformers.append((
            "num",
            Pipeline([
                ("imp", SimpleImputer(strategy="median")),
                ("scl", StandardScaler()),
            ]),
            numeric_cols,
        ))

    preprocessor = ColumnTransformer(transformers=transformers, remainder="drop")

    lr = LogisticRegression(
        C=C,
        penalty=penalty,
        solver="liblinear",
        class_weight="balanced",
        max_iter=1000,
        random_state=RANDOM_SEED,
    )

    return Pipeline([("prep", preprocessor), ("clf", lr)])


# ---------------------------------------------------------------------------
# 4. Evaluación y umbral óptimo
# ---------------------------------------------------------------------------
def optimal_threshold(probs: np.ndarray, labels: np.ndarray) -> float:
    """Umbral que maximiza F1 sobre el conjunto dado."""
    precisions, recalls, thresholds = precision_recall_curve(labels, probs)
    f1s = np.where(
        (precisions[:-1] + recalls[:-1]) == 0,
        0.0,
        2 * precisions[:-1] * recalls[:-1] / (precisions[:-1] + recalls[:-1] + 1e-9),
    )
    return float(thresholds[np.argmax(f1s)])


def evaluate(
    model: Pipeline,
    X: pd.DataFrame,
    y: pd.Series,
    threshold: float | None = None,
) -> dict:
    """Métricas completas sobre un split. Si threshold es None se optimiza en este split."""
    probs = model.predict_proba(X)[:, 1]
    if threshold is None:
        threshold = optimal_threshold(probs, y.values)
    preds = (probs >= threshold).astype(int)
    return {
        "auc_pr":    float(average_precision_score(y, probs)),
        "auc_roc":   float(roc_auc_score(y, probs)),
        "f1":        float(f1_score(y, preds, zero_division=0)),
        "precision": float(precision_score(y, preds, zero_division=0)),
        "recall":    float(recall_score(y, preds, zero_division=0)),
        "threshold": float(threshold),
        "n":         int(len(y)),
        "prevalence": float(y.mean()),
    }


# ---------------------------------------------------------------------------
# 5. Grid search con logging jerárquico en MLflow
# ---------------------------------------------------------------------------
def run_grid_search(
    X_train: pd.DataFrame, y_train: pd.Series,
    X_val:   pd.DataFrame, y_val:   pd.Series,
    feature_cols: list[str],
    parent_run_id: str,
    gold_version: int,
    n_unlabeled: int,
    date_train_end,
    date_val_end,
) -> str:
    """
    Entrena un run hijo por combinación de hiperparámetros.
    Devuelve el run_id del hijo con mayor AUC-PR en validación.
    """
    best_auc_pr   = -1.0
    best_child_id: str = ""

    for params in PARAM_GRID:
        run_name = f"C={params['C']}_pen={params['penalty']}"
        with mlflow.start_run(run_name=run_name, nested=True) as child:

            # Hiperparámetros y linaje de datos
            mlflow.log_params({
                **params,
                "parent_run_id":  parent_run_id,
                "train_end":      str(date_train_end),
                "val_end":        str(date_val_end),
                "n_train":        len(X_train),
                "n_val":          len(X_val),
                "n_features":     len(feature_cols),
                "n_unlabeled":    n_unlabeled,
                "gold_path":      _gold_path(),
                "gold_version":   gold_version,
                "random_seed":    RANDOM_SEED,
            })

            model = build_pipeline(feature_cols, **params)
            model.fit(X_train, y_train)

            # Umbral optimizado en validación (nunca en train ni test)
            val_probs = model.predict_proba(X_val)[:, 1]
            val_threshold = optimal_threshold(val_probs, y_val.values)

            train_m = evaluate(model, X_train, y_train, threshold=val_threshold)
            val_m   = evaluate(model, X_val,   y_val,   threshold=val_threshold)

            mlflow.log_metrics({f"train_{k}": v for k, v in train_m.items()})
            mlflow.log_metrics({f"val_{k}":   v for k, v in val_m.items()})

            # Artefacto del modelo
            sig = mlflow.models.infer_signature(X_train, model.predict(X_train))
            mlflow.sklearn.log_model(
                model, "model",
                input_example=X_train.head(5),
                signature=sig,
            )

            log.info(
                "  [%s]  val_auc_pr=%.4f  val_auc_roc=%.4f  val_f1=%.4f  thr=%.3f",
                run_name,
                val_m["auc_pr"], val_m["auc_roc"], val_m["f1"], val_m["threshold"],
            )

            if val_m["auc_pr"] > best_auc_pr:
                best_auc_pr   = val_m["auc_pr"]
                best_child_id = child.info.run_id

    log.info("Mejor hijo: %s  (val_auc_pr=%.4f)", best_child_id, best_auc_pr)
    return best_child_id


# ---------------------------------------------------------------------------
# 6. Registro de modelo con alias en MLflow Model Registry
# ---------------------------------------------------------------------------
def _ensure_registered_model(client: mlflow.MlflowClient) -> None:
    try:
        client.create_registered_model(MODEL_NAME)
        log.info("Modelo registrado creado: %s", MODEL_NAME)
    except mlflow.exceptions.MlflowException:
        pass  # ya existe


def _get_champion(client: mlflow.MlflowClient):
    """Devuelve la ModelVersion con alias 'champion', o None si no existe."""
    try:
        return client.get_model_version_by_alias(MODEL_NAME, "champion")
    except mlflow.exceptions.MlflowException:
        return None


def _child_params(client: mlflow.MlflowClient, run_id: str) -> dict:
    run = client.get_run(run_id)
    return {
        "C":       float(run.data.params["C"]),
        "penalty": run.data.params["penalty"],
    }


def _log_and_register(
    model: Pipeline,
    alias: str,
    params: dict,
    test_metrics: dict,
    X_all: pd.DataFrame,
    y_all: pd.Series,
    extra_params: dict,
) -> None:
    """Registra un modelo en MLflow Model Registry y le asigna el alias dado."""
    client = mlflow.MlflowClient()
    run_name = f"{alias}_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"

    with mlflow.start_run(run_name=run_name) as reg_run:
        mlflow.log_params({**params, **extra_params, "alias": alias})
        mlflow.log_metrics({f"test_{k}": v for k, v in test_metrics.items()})

        sig = mlflow.models.infer_signature(X_all, model.predict(X_all))
        mlflow.sklearn.log_model(
            model, "model",
            input_example=X_all.head(5),
            signature=sig,
        )
        model_uri = f"runs:/{reg_run.info.run_id}/model"

    _ensure_registered_model(client)
    version = mlflow.register_model(model_uri, MODEL_NAME)
    client.set_registered_model_alias(MODEL_NAME, alias, version.version)
    log.info(
        "Modelo '%s' v%s registrado con alias '%s'  (auc_pr_test=%.4f)",
        MODEL_NAME, version.version, alias, test_metrics["auc_pr"],
    )


# ---------------------------------------------------------------------------
# 7. Promoción champion-challenger
# ---------------------------------------------------------------------------
def _try_delete_alias(client: mlflow.MlflowClient, alias: str) -> None:
    """Elimina un alias del modelo registrado; silencia errores en MLflow < 2.7."""
    try:
        client.delete_registered_model_alias(MODEL_NAME, alias)
    except Exception:
        pass


def promote_champion(
    best_child_id: str,
    X_train: pd.DataFrame, y_train: pd.Series,
    X_val:   pd.DataFrame, y_val:   pd.Series,
    X_test:  pd.DataFrame, y_test:  pd.Series,
    feature_cols: list[str],
    parent_run_id: str,
    gold_version: int,
    n_unlabeled: int,
    date_train_end,
    date_val_end,
) -> None:
    """
    Evalúa el mejor candidato sobre test y decide si promoverlo a champion.

    Flujo champion-challenger:
      1. Candidato validado → alias 'challenger' en el registro (antes de ver test).
      2. Evaluado sobre test; umbral fijado en val (nunca en test).
      3a. No supera umbrales mínimos → 'challenger' pasa a 'rejected'.
      3b. Existe champion y challenger ≤ champion → 'challenger' pasa a 'rejected'.
      3c. No existe champion o challenger > champion → retrain en todos los datos
          → nuevo 'champion'; champion anterior → 'retired'; alias 'challenger' eliminado.
    """
    client = mlflow.MlflowClient()

    # Cargar candidato desde el run hijo ganador
    candidate = mlflow.sklearn.load_model(f"runs:/{best_child_id}/model")

    # ---- Registrar como 'challenger' ANTES de evaluar en test --------------
    _ensure_registered_model(client)
    challenger_reg = mlflow.register_model(f"runs:/{best_child_id}/model", MODEL_NAME)
    challenger_ver = challenger_reg.version
    client.set_registered_model_alias(MODEL_NAME, "challenger", challenger_ver)
    log.info("Candidato registrado como 'challenger' v%s", challenger_ver)

    # Umbral fijado en validación (no se reoptimiza en test)
    val_probs     = candidate.predict_proba(X_val)[:, 1]
    val_threshold = optimal_threshold(val_probs, y_val.values)
    test_m = evaluate(candidate, X_test, y_test, threshold=val_threshold)

    log.info(
        "Challenger en test:  auc_pr=%.4f  auc_roc=%.4f  f1=%.4f",
        test_m["auc_pr"], test_m["auc_roc"], test_m["f1"],
    )

    # ---- Comprobar umbrales mínimos ----------------------------------------
    passes = (test_m["auc_pr"] >= MIN_AUC_PR) and (test_m["auc_roc"] >= MIN_AUC_ROC)
    if not passes:
        log.warning(
            "Challenger NO supera umbrales mínimos "
            "(auc_pr>=%.2f, auc_roc>=%.2f). 'challenger' → 'rejected'.",
            MIN_AUC_PR, MIN_AUC_ROC,
        )
        client.set_registered_model_alias(MODEL_NAME, "rejected", challenger_ver)
        _try_delete_alias(client, "challenger")
        return

    best_params = _child_params(client, best_child_id)
    extra = {
        "parent_run_id":   parent_run_id,
        "best_child_id":   best_child_id,
        "train_end":       str(date_train_end),
        "val_end":         str(date_val_end),
        "gold_path":       _gold_path(),
        "gold_version":    gold_version,
        "n_unlabeled":     n_unlabeled,
        "random_seed":     RANDOM_SEED,
    }

    # ---- Comprobar si ya existe un champion --------------------------------
    current_champion = _get_champion(client)

    if current_champion is not None:
        # Comparar challenger contra champion en test con el mismo umbral
        champion_model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@champion")
        current_m = evaluate(champion_model, X_test, y_test, threshold=val_threshold)
        log.info(
            "Champion actual en test:  auc_pr=%.4f  auc_roc=%.4f",
            current_m["auc_pr"], current_m["auc_roc"],
        )

        if test_m["auc_pr"] <= current_m["auc_pr"]:
            log.info("Challenger NO supera al champion. 'challenger' → 'rejected'.")
            client.set_registered_model_alias(MODEL_NAME, "rejected", challenger_ver)
            _try_delete_alias(client, "challenger")
            return

        # El challenger gana → el champion anterior pasa a 'retired'
        log.info(
            "Challenger supera al champion. Champion anterior v%s → 'retired'.",
            current_champion.version,
        )
        client.set_registered_model_alias(MODEL_NAME, "retired", current_champion.version)

    # ---- Reentrenar en todos los datos -------------------------------------
    log.info("Reentrenando en train+val+test antes de registrar como champion...")
    X_all = pd.concat([X_train, X_val, X_test])
    y_all = pd.concat([y_train, y_val, y_test])
    champion_model = build_pipeline(feature_cols, **best_params)
    champion_model.fit(X_all, y_all)
    log.info("Reentrenamiento completado (%d muestras).", len(X_all))

    _try_delete_alias(client, "challenger")
    _log_and_register(champion_model, "champion", best_params, test_m, X_all, y_all, extra)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(TRAINING_EXPERIMENT)

    log.info("MLflow URI:   %s", MLFLOW_TRACKING_URI)
    log.info("Experimento:  %s", TRAINING_EXPERIMENT)
    log.info("Gold path:    %s", _gold_path())

    # 1. Cargar tabla Gold
    df, gold_version = load_gold_table()

    # 2. Particionar temporalmente (cortes dinámicos por percentil)
    (X_tr, y_tr, X_va, y_va, X_te, y_te,
     feature_cols, n_unlabeled,
     date_train_end, date_val_end) = split_temporal(df)

    # 3. Grid search con logging jerárquico
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    with mlflow.start_run(run_name=f"grid_search_{timestamp}") as parent:
        mlflow.log_params({
            "train_end":        str(date_train_end),
            "val_end":          str(date_val_end),
            "split_p70":        TRAIN_PERCENTILE,
            "split_p90":        VAL_PERCENTILE,
            "n_train":          len(X_tr),
            "n_val":            len(X_va),
            "n_test":           len(X_te),
            "n_unlabeled":      n_unlabeled,
            "n_features":       len(feature_cols),
            "gold_path":        _gold_path(),
            "gold_version":     gold_version,
            "random_seed":      RANDOM_SEED,
            "grid_size":        len(PARAM_GRID),
            "selection_metric": "val_auc_pr",
        })

        log.info("Iniciando grid search: %d combinaciones", len(PARAM_GRID))
        best_child_id = run_grid_search(
            X_tr, y_tr, X_va, y_va,
            feature_cols, parent.info.run_id,
            gold_version, n_unlabeled,
            date_train_end, date_val_end,
        )
        mlflow.log_param("best_child_run_id", best_child_id)

    log.info("Grid search completado.")

    # 4. Evaluación y promoción a champion
    log.info("Evaluando candidato para promoción a champion...")
    promote_champion(
        best_child_id,
        X_tr, y_tr, X_va, y_va, X_te, y_te,
        feature_cols, parent.info.run_id,
        gold_version, n_unlabeled,
        date_train_end, date_val_end,
    )

    log.info("Fase 4 completada.")


if __name__ == "__main__":
    main()
