"""
Generador de datos sintéticos — paradigma caudal de pacientes únicos.

Todos los parámetros se controlan desde dags/pipeline_config.py / variables
de entorno. Ver también: spark-apps/pipeline/config.py para DATE_COLS.

Esquema de timestamps
---------------------
- updated_at      (clínica, watermark Bronze):  epoch + i/PATIENTS_PER_MINUTE
- survey_date     (sppb, lifestyle, watermark):  updated_at + 0-30 min (lag operativo)
- session_timestamp (gait, watermark):           updated_at + 0-30 min
- updated_at      (labels, watermark):           clinical.updated_at + 191-822 min
                                                 (≡ 7-30 días calendario escalados)
- snapshot_date   (clínica/labels):              fecha real de evaluación
                                                 distribuida sobre 1 año
- label_available_date (labels):                 snapshot_date + 7-30 días calendario
                                                 (para el filtro anti-leakage del training)

Salida: synthetic_data/
  source_a/clinical_records/batch_NNN.csv
  source_b1/sppb_surveys/batch_NNN.jsonl
  source_b2/lifestyle_surveys/batch_NNN.jsonl
  source_c/gait_events/batch_NNN.jsonl
  labels/batch_NNN.csv

Uso:
    python generate_frailty.py
    PATIENTS_PER_MINUTE=20 TOTAL_PATIENTS=50000 python generate_frailty.py
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path

# ── Config (mismas vars que dags/pipeline_config.py) ─────────────────────────
SEED                = 42
random.seed(SEED)

PATIENTS_PER_MINUTE = int(os.getenv("PATIENTS_PER_MINUTE", "10"))
TOTAL_PATIENTS      = int(os.getenv("TOTAL_PATIENTS",      "100000"))

# Epoch de pipeline: cuando el primer registro llega al sistema
PIPELINE_EPOCH      = datetime(2024, 1, 1, 0, 0, 0)

# Ventana de evaluación clínica real (12 meses de 2024)
EVAL_START          = date(2024, 1, 1)
EVAL_DAYS           = 365

# Lag de fuentes secundarias (en minutos de pipeline-time)
LAG_SECONDARY_MIN   = 0      # sppb, lifestyle, gait llegan muy pronto
LAG_SECONDARY_MAX   = 30     # ... máx 30 min de pipeline tras clinical

# Lag de labels en pipeline-time, escalado desde 7-30 días calendario:
#   1 año (365 días) ≡ TOTAL_PATIENTS/PATIENTS_PER_MINUTE pipeline-min
#   → 1 día ≡ (TOTAL_PATIENTS/PATIENTS_PER_MINUTE) / 365 pipeline-min
#   Con 100k y 10/min → 10000/365 ≈ 27.4 min/día
#   7  días ≡ 191 min,  30 días ≡ 822 min
_PIPELINE_MIN_PER_DAY = (TOTAL_PATIENTS / PATIENTS_PER_MINUTE) / EVAL_DAYS
LAG_LABEL_MIN       = int(7  * _PIPELINE_MIN_PER_DAY)   # ≈ 191
LAG_LABEL_MAX       = int(30 * _PIPELINE_MIN_PER_DAY)   # ≈ 822

STRIDES_PER_SESSION = 20
BATCH_SIZE          = 10_000
OUTPUT_DIR          = Path("synthetic_data")


# ── Helpers ───────────────────────────────────────────────────────────────────

def patient_id(index: int, seed: int = SEED) -> str:
    h = hashlib.md5(f"{seed}:{index}".encode()).hexdigest().upper()
    return f"PAT_{h[:8]}"


def iso_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def gauss_clamp(mu, sigma, lo, hi) -> float:
    for _ in range(200):
        v = random.gauss(mu, sigma)
        if lo <= v <= hi:
            return v
    return max(lo, min(hi, mu))


def weighted_choice(options, weights):
    total = sum(weights)
    r = random.random() * total
    cum = 0.0
    for opt, w in zip(options, weights):
        cum += w
        if r <= cum:
            return opt
    return options[-1]


# ── Generación por paciente ───────────────────────────────────────────────────

def _clinical_row(pid: str, snap: date, arr: datetime, frail: bool) -> dict:
    sex    = random.choice(["M", "F"])
    age    = int(gauss_clamp(75 if frail else 72, 7, 65, 100))
    bmi    = round(gauss_clamp(27 if frail else 25, 4, 17, 45), 1)
    sbp    = int(gauss_clamp(140 if frail else 130, 18, 90, 200))
    dbp    = int(gauss_clamp(80, 10, 55, 120))
    gfr    = round(gauss_clamp(55 if frail else 72, 18, 15, 120), 1)
    alb    = round(gauss_clamp(3.5 if frail else 4.0, 0.5, 2.0, 5.5), 1)
    hgb    = round(gauss_clamp(12.5 if frail else 14.0, 1.5, 8.0, 18.0), 1)
    comor  = max(0, int(gauss_clamp(3 if frail else 1, 1.5, 0, 10)))
    polym  = max(0, int(gauss_clamp(5 if frail else 2, 2, 0, 15)))
    fall12 = random.randint(1, 4) if frail else 0
    hosp12 = random.randint(0, 3) if frail else 0
    mmse   = int(gauss_clamp(22 if frail else 27, 4, 0, 30))
    dep    = round(random.uniform(0.3, 1.0) if frail else random.uniform(0.0, 0.4), 2)
    return {
        "patient_id":       pid,
        "snapshot_date":    snap.isoformat(),
        "updated_at":       iso_ts(arr),
        "age":              age,
        "sex":              sex,
        "bmi":              bmi,
        "systolic_bp":      sbp,
        "diastolic_bp":     dbp,
        "gfr":              gfr,
        "albumin":          alb,
        "hemoglobin":       hgb,
        "comorbidity_index": comor,
        "polypharmacy":     polym,
        "falls_last_12m":   fall12,
        "hospitalizations_last_12m": hosp12,
        "mmse_score":       mmse,
        "depression_score": dep,
    }


def _sppb_row(pid: str, snap: date, arr: datetime, frail: bool) -> dict:
    balance = round(gauss_clamp(2.5 if frail else 3.8, 1.0, 0, 4), 1)
    gait_s  = round(gauss_clamp(5.5 if frail else 3.5, 1.5, 1, 15), 1)
    chair   = round(gauss_clamp(16 if frail else 11, 4, 5, 60), 1)
    total   = max(0, min(12, int(round(
        (balance / 4) * 4 + (1 if gait_s < 4.5 else 2 if gait_s < 7 else 3 if gait_s < 10 else 4) +
        (1 if chair > 16.7 else 2 if chair > 13.7 else 3 if chair > 11.2 else 4)
    ))))
    rid = hashlib.md5(f"sppb:{pid}:{snap}".encode()).hexdigest()[:12].upper()
    return {
        "response_id":   f"SPPB_{rid}",
        "patient_id":    pid,
        "survey_date":   iso_ts(arr),
        "sppb_balance":  balance,
        "sppb_gait_speed_s": gait_s,
        "sppb_chair_stand_s": chair,
        "sppb_total":    total,
    }


def _lifestyle_row(pid: str, snap: date, arr: datetime, frail: bool) -> dict:
    steps     = int(gauss_clamp(3000 if frail else 6500, 2000, 0, 20000))
    exercise  = round(gauss_clamp(60 if frail else 200, 80, 0, 600), 0)
    protein   = round(gauss_clamp(0.7 if frail else 1.0, 0.2, 0.3, 2.0), 2)
    social    = int(gauss_clamp(2 if frail else 5, 2, 0, 10))
    tobacco   = weighted_choice([0, 1, 2], [0.55, 0.15, 0.30])
    alcohol   = weighted_choice([0, 1, 2, 3], [0.40, 0.30, 0.20, 0.10])
    rid = hashlib.md5(f"lifestyle:{pid}:{snap}".encode()).hexdigest()[:12].upper()
    return {
        "response_id":          f"LS_{rid}",
        "patient_id":           pid,
        "survey_date":          iso_ts(arr),
        "steps_per_day":        steps,
        "moderate_exercise_min_week": int(exercise),
        "protein_intake_g_per_kg": protein,
        "social_contacts_per_week": social,
        "tobacco_use":          tobacco,
        "alcohol_units_per_week": alcohol,
    }


def _gait_rows(pid: str, snap: date, arr: datetime, frail: bool) -> list[dict]:
    sid = hashlib.md5(f"gait:{pid}:{snap}".encode()).hexdigest()[:12].upper()
    speed   = round(gauss_clamp(0.6 if frail else 1.1, 0.25, 0.1, 2.0), 3)
    cadence = round(gauss_clamp(85 if frail else 105, 15, 40, 150), 1)
    rows = []
    for s in range(STRIDES_PER_SESSION):
        eid = hashlib.md5(f"event:{pid}:{sid}:{s}".encode()).hexdigest()[:12].upper()
        rows.append({
            "event_id":          f"GE_{eid}",
            "patient_id":        pid,
            "session_id":        f"GS_{sid}",
            "session_timestamp": iso_ts(arr + timedelta(seconds=s * 2)),
            "stride_length_m":   round(gauss_clamp(0.55 if frail else 0.70, 0.08, 0.2, 1.2), 3),
            "stride_time_s":     round(gauss_clamp(1.3 if frail else 0.95, 0.15, 0.5, 2.5), 3),
            "cadence_steps_min": round(gauss_clamp(cadence, 5, 40, 150), 1),
            "gait_speed_m_s":    round(gauss_clamp(speed, 0.05, 0.1, 2.0), 3),
            "asymmetry_index":   round(gauss_clamp(0.12 if frail else 0.05, 0.04, 0.0, 0.5), 3),
            "double_support_pct": round(gauss_clamp(28 if frail else 20, 5, 10, 60), 1),
        })
    return rows


def _label_row(pid: str, snap: date, label_date: date, arr_ts: datetime, frail: bool) -> dict:
    return {
        "patient_id":          pid,
        "snapshot_date":       snap.isoformat(),
        "label_available_date": label_date.isoformat(),
        "frailty_label":       int(frail),
        "updated_at":          iso_ts(arr_ts),
    }


# ── Función principal ─────────────────────────────────────────────────────────

def generate(output_dir: Path = OUTPUT_DIR) -> None:
    # Crear directorios
    dirs = {
        "clinical":   output_dir / "source_a" / "clinical_records",
        "sppb":       output_dir / "source_b1" / "sppb_surveys",
        "lifestyle":  output_dir / "source_b2" / "lifestyle_surveys",
        "gait":       output_dir / "source_c" / "gait_events",
        "labels":     output_dir / "labels",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)

    n_batches = (TOTAL_PATIENTS + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_idx in range(n_batches):
        start_i = batch_idx * BATCH_SIZE
        end_i   = min(start_i + BATCH_SIZE, TOTAL_PATIENTS)

        batch_tag = f"batch_{batch_idx + 1:03d}"
        print(f"  Generando {batch_tag} (pacientes {start_i:,}–{end_i-1:,})…")

        clin_rows  = []
        sppb_rows  = []
        life_rows  = []
        gait_rows  = []
        label_rows = []

        for i in range(start_i, end_i):
            pid = patient_id(i)

            # Prevalencia de fragilidad ~25%
            frail = (random.random() < 0.25)

            # ── Timestamps de pipeline ───────────────────────────────────────
            # Llegada clínica al sistema (a ritmo PATIENTS_PER_MINUTE)
            clin_arr = PIPELINE_EPOCH + timedelta(minutes=i / PATIENTS_PER_MINUTE)

            # Fuentes secundarias: llegan 0-30 min después en pipeline-time
            sec_lag      = timedelta(minutes=random.randint(LAG_SECONDARY_MIN, LAG_SECONDARY_MAX))
            sppb_arr     = clin_arr + sec_lag
            life_arr     = clin_arr + timedelta(minutes=random.randint(LAG_SECONDARY_MIN, LAG_SECONDARY_MAX))
            gait_arr     = clin_arr + timedelta(minutes=random.randint(LAG_SECONDARY_MIN, LAG_SECONDARY_MAX))

            # Labels: llegan 7-30 días calendario (escalados a pipeline-time)
            label_lag = timedelta(minutes=random.randint(LAG_LABEL_MIN, LAG_LABEL_MAX))
            label_arr = clin_arr + label_lag

            # ── Fechas de evaluación (calendario real, distribuidas en 1 año) ──
            base_day = (i / TOTAL_PATIENTS) * EVAL_DAYS
            jitter   = random.randint(-15, 15)
            snap_day = int(max(0, min(EVAL_DAYS - 1, base_day + jitter)))
            snap     = EVAL_START + timedelta(days=snap_day)

            # label_available_date = snapshot + 7-30 días calendario (anti-leakage)
            label_delay_days  = random.randint(7, 30)
            label_avail_date  = snap + timedelta(days=label_delay_days)

            clin_rows.append(_clinical_row(pid, snap, clin_arr, frail))
            sppb_rows.append(_sppb_row(pid, snap, sppb_arr, frail))
            life_rows.append(_lifestyle_row(pid, snap, life_arr, frail))
            gait_rows.extend(_gait_rows(pid, snap, gait_arr, frail))
            label_rows.append(_label_row(pid, snap, label_avail_date, label_arr, frail))

        # ── Escribir ficheros ─────────────────────────────────────────────────
        # Clinical CSV
        clin_path = dirs["clinical"] / f"{batch_tag}.csv"
        with open(clin_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(clin_rows[0].keys()))
            w.writeheader()
            w.writerows(clin_rows)

        # SPPB JSONL
        sppb_path = dirs["sppb"] / f"{batch_tag}.jsonl"
        with open(sppb_path, "w") as f:
            for r in sppb_rows:
                f.write(json.dumps(r) + "\n")

        # Lifestyle JSONL
        life_path = dirs["lifestyle"] / f"{batch_tag}.jsonl"
        with open(life_path, "w") as f:
            for r in life_rows:
                f.write(json.dumps(r) + "\n")

        # Gait JSONL
        gait_path = dirs["gait"] / f"{batch_tag}.jsonl"
        with open(gait_path, "w") as f:
            for r in gait_rows:
                f.write(json.dumps(r) + "\n")

        # Labels CSV
        label_path = dirs["labels"] / f"{batch_tag}.csv"
        with open(label_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(label_rows[0].keys()))
            w.writeheader()
            w.writerows(label_rows)

    print(f"\n✓ {TOTAL_PATIENTS:,} pacientes generados en {output_dir}/")
    total_gait = TOTAL_PATIENTS * STRIDES_PER_SESSION
    print(f"  Clinical:  {TOTAL_PATIENTS:,} filas")
    print(f"  SPPB:      {TOTAL_PATIENTS:,} filas")
    print(f"  Lifestyle: {TOTAL_PATIENTS:,} filas")
    print(f"  Gait:      {total_gait:,} filas ({STRIDES_PER_SESSION} strides/paciente)")
    print(f"  Labels:    {TOTAL_PATIENTS:,} filas")
    total_pipeline_min = TOTAL_PATIENTS / PATIENTS_PER_MINUTE
    print(f"\n  Pipeline-time span: {total_pipeline_min:,.0f} min"
          f" ≈ {total_pipeline_min/1440:.1f} pipeline-días")
    print(f"  Labels span hasta: {total_pipeline_min + LAG_LABEL_MAX:,.0f} min"
          f" ≈ {(total_pipeline_min + LAG_LABEL_MAX)/1440:.1f} pipeline-días")
    print(f"\n  LAG_LABEL_MIN={LAG_LABEL_MIN} min, LAG_LABEL_MAX={LAG_LABEL_MAX} min")
    print(f"  (7-30 días calendario escalados a pipeline-time)")


if __name__ == "__main__":
    generate()
