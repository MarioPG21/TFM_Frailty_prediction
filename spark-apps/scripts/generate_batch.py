#!/usr/bin/env python3
"""
Generador de lotes adicionales de pacientes sintéticos.

Produce la misma estructura de ficheros que generate_frailty.py pero con:
  - Rango de evaluación configurable (--eval-start / --eval-end)
  - Semilla configurable (--seed)
  - Directorio de salida configurable (--output-dir)
  - IDs únicos: el prefijo de MD5 incluye la semilla, por lo que no colisionan
    con los IDs del lote base (semilla 42).

Uso:
    # Batch 2: 3 000 pacientes, Jan-Jun 2025
    python generate_batch.py --n 3000 --seed 123 \
        --eval-start 2025-01-01 --eval-end 2025-06-30 \
        --output-dir synthetic_data_batch2

    # Batch 3: 2 000 pacientes, Jul-Dic 2025
    python generate_batch.py --n 2000 --seed 456 \
        --eval-start 2025-07-01 --eval-end 2025-12-31 \
        --output-dir synthetic_data_batch3
"""

import argparse
import csv
import hashlib
import json
import os
import random
from datetime import date, timedelta

# Offsets de llegada al sistema (días desde evaluation_date, por fuente)
ARRIVAL_CLINICAL_MAX  = 2
ARRIVAL_GAIT_MAX      = 5
ARRIVAL_SPPB_MIN      = 7
ARRIVAL_SPPB_MAX      = 30
ARRIVAL_LIFESTYLE_MIN = 7
ARRIVAL_LIFESTYLE_MAX = 30
LABEL_DELAY_MIN       = 7
LABEL_DELAY_MAX       = 30
STRIDES_PER_SESSION   = 20

_AGE_PARAMS    = {0: (72, 5,  65,  85),  1: (77, 6,  65,  90),  2: (82, 6,  65,  95)}
_HEIGHT_PARAMS = {0: (167, 9, 145, 190), 1: (166, 9, 145, 190), 2: (164, 9, 145, 185)}
_WEIGHT_PARAMS = {0: (72, 12, 45, 105),  1: (68, 13, 42, 105),  2: (63, 13, 40, 100)}
_HR_PARAMS     = {0: (68,  8,  50,  90), 1: (72,  9,  50,  95), 2: (76, 10,  50, 100)}
_TUG_PARAMS    = {0: ( 9,  2,   5,  14), 1: (13,  3,   8,  22), 2: (20,  6,  10,  60)}
_GRIP_PARAMS   = {0: (32,  6,  18,  50), 1: (24,  5,  12,  38), 2: (16,  5,   5,  28)}
_MMSE_PARAMS   = {0: (28,  1.5, 24, 30), 1: (26,  2,  20,  30), 2: (23,  3,  10,  29)}
_GDS_PARAMS    = {0: (1.5, 0.5,  1,  3), 1: (2.5, 1,   1,   5), 2: ( 4,  1.5, 1,   7)}
_FI_PARAMS     = {0: (0.10, 0.04, 0.0, 0.20), 1: (0.22, 0.04, 0.10, 0.35), 2: (0.38, 0.07, 0.25, 0.70)}
_FRIED_PROBS   = {
    "fried_weight_loss":  {0: 0.05, 1: 0.20, 2: 0.55},
    "fried_weakness":     {0: 0.08, 1: 0.35, 2: 0.75},
    "fried_slowness":     {0: 0.06, 1: 0.30, 2: 0.70},
    "fried_low_activity": {0: 0.15, 1: 0.45, 2: 0.80},
    "fried_exhaustion":   {0: 0.10, 1: 0.35, 2: 0.65},
}
_SPPB_BALANCE = {0: ([3, 4], [0.3, 0.7]), 1: ([2, 3, 4], [0.3, 0.5, 0.2]), 2: ([0, 1, 2, 3], [0.2, 0.3, 0.3, 0.2])}
_SPPB_GAIT    = {0: ([3, 4], [0.4, 0.6]), 1: ([2, 3], [0.5, 0.5]), 2: ([0, 1, 2], [0.3, 0.4, 0.3])}
_SPPB_CHAIR   = {0: ([3, 4], [0.4, 0.6]), 1: ([1, 2, 3], [0.3, 0.4, 0.3]), 2: ([0, 1, 2], [0.4, 0.4, 0.2])}
_FES_PARAMS   = {0: (10, 2, 7, 16), 1: (16, 3, 9, 24), 2: (22, 3, 14, 28)}
_FALLS_PROBS  = {0: 0.10, 1: 0.28, 2: 0.52}
_PA_VIG_PARAMS = {0: (2, 1, 0, 7), 1: (1, 1, 0, 5), 2: (0.3, 0.5, 0, 3)}
_PA_MOD_PARAMS = {0: (4, 1.5, 0, 7), 1: (2.5, 1.5, 0, 7), 2: (1, 1, 0, 5)}
_SED_PARAMS    = {0: (5, 2, 1, 10), 1: (8, 2.5, 2, 14), 2: (11, 2.5, 4, 18)}
_DEPR_P = {0: 0.08, 1: 0.22, 2: 0.45}
_HTN_P  = {0: 0.30, 1: 0.50, 2: 0.65}
_DIA_P  = {0: 0.12, 1: 0.22, 2: 0.35}
_ART_P  = {0: 0.15, 1: 0.35, 2: 0.60}
_STRIDE_DUR_BASE  = {0: (1.05, 0.08, 0.85, 1.25), 1: (1.25, 0.10, 1.00, 1.60), 2: (1.55, 0.15, 1.20, 2.50)}
_STRIDE_LEN_BASE  = {0: (1.10, 0.12, 0.80, 1.45), 1: (0.85, 0.12, 0.55, 1.15), 2: (0.60, 0.12, 0.20, 0.90)}
_FOOT_CLEAR_BASE  = {0: (0.12, 0.02, 0.06, 0.18), 1: (0.08, 0.02, 0.04, 0.14), 2: (0.05, 0.02, 0.01, 0.10)}
_TOE_OFF_BASE     = {0: (-16, 3, -25, -5), 1: (-10, 4, -20, 0), 2: (-5, 4, -15, 5)}
_HEEL_STRIKE_BASE = {0: (10, 3, 2, 18), 1: (6, 3, -2, 14), 2: (2, 4, -10, 12)}
_LATERAL_BASE     = {0: (0.04, 0.01, 0.01, 0.07), 1: (0.07, 0.02, 0.03, 0.12), 2: (0.11, 0.03, 0.05, 0.20)}
_SIGMA_INTRA_DUR  = {0: (0.030, 0.008, 0.010, 0.060), 1: (0.060, 0.015, 0.025, 0.110), 2: (0.110, 0.025, 0.050, 0.220)}
_SWING_PROP       = {0: (0.38, 0.02, 0.32, 0.44), 1: (0.34, 0.02, 0.28, 0.40), 2: (0.30, 0.03, 0.24, 0.38)}

_A_FIELDNAMES = [
    "patient_id", "snapshot_date", "updated_at", "age", "sex",
    "height_cm", "weight_kg", "bmi", "heart_rate_bpm",
    "fried_weight_loss", "fried_weakness", "fried_slowness",
    "fried_low_activity", "fried_exhaustion",
    "tug_time_s", "grip_strength_kg", "mmse", "gds", "frailty_index_fi",
]
_LABELS_FIELDNAMES = ["patient_id", "snapshot_date", "label_available_date", "frailty_label"]


def _gauss_clamp(mu, sigma, lo, hi):
    for _ in range(200):
        v = random.gauss(mu, sigma)
        if lo <= v <= hi:
            return v
    return max(lo, min(hi, random.gauss(mu, sigma)))


def _weighted_choice(options, weights):
    total = sum(weights)
    r = random.random() * total
    cum = 0.0
    for opt, w in zip(options, weights):
        cum += w
        if r <= cum:
            return opt
    return options[-1]


def _bernoulli(p):
    return 1 if random.random() < p else 0


def _make_id(prefix, index, seed):
    h = hashlib.md5(f"{prefix}{index}{seed}".encode()).hexdigest()[:8].upper()
    return f"{prefix}_{h}"


def _iso_ts(d, hour, minute, second=0):
    return f"{d.isoformat()}T{hour:02d}:{minute:02d}:{second:02d}Z"


def build_population(n, seed, eval_start, eval_end):
    random.seed(seed)
    eval_span = (eval_end - eval_start).days
    patients = []
    for i in range(n):
        label3 = _weighted_choice([0, 1, 2], [0.40, 0.35, 0.25])
        sex    = _weighted_choice(["M", "F"], [0.45, 0.55])
        age    = int(_gauss_clamp(*_AGE_PARAMS[label3]))
        dob    = date(eval_start.year - age, 7, 1)
        height = round(_gauss_clamp(*_HEIGHT_PARAMS[label3]), 1)
        weight = round(_gauss_clamp(*_WEIGHT_PARAMS[label3]), 1)
        eval_date = eval_start + timedelta(days=random.randint(0, eval_span))
        patients.append({
            "patient_id":         _make_id("PAT", i, seed),
            "sex":                sex,
            "dob":                dob,
            "_state":             label3,
            "frailty_label":      0 if label3 < 2 else 1,
            "height_cm":          height,
            "base_weight":        weight,
            "evaluation_date":    eval_date,
            "_arrival_clinical":  eval_date + timedelta(days=random.randint(0, ARRIVAL_CLINICAL_MAX)),
            "_arrival_gait":      eval_date + timedelta(days=random.randint(0, ARRIVAL_GAIT_MAX)),
            "_arrival_sppb":      eval_date + timedelta(days=random.randint(ARRIVAL_SPPB_MIN, ARRIVAL_SPPB_MAX)),
            "_arrival_lifestyle": eval_date + timedelta(days=random.randint(ARRIVAL_LIFESTYLE_MIN, ARRIVAL_LIFESTYLE_MAX)),
            "_label_available":   eval_date + timedelta(days=random.randint(LABEL_DELAY_MIN, LABEL_DELAY_MAX)),
            "session_jitter":     random.randint(-3, 3),
        })
    return patients


def generate_source_a(patients, output_dir):
    base_a = os.path.join(output_dir, "source_a", "clinical_records")
    base_l = os.path.join(output_dir, "labels")
    os.makedirs(base_a, exist_ok=True)
    os.makedirs(base_l, exist_ok=True)
    handles_a, handles_l, writers_a, writers_l, counts = {}, {}, {}, {}, {}

    for patient in patients:
        ym = patient["_arrival_clinical"].strftime("%Y-%m")
        if ym not in handles_a:
            handles_a[ym] = open(os.path.join(base_a, f"{ym}.csv"), "w", newline="", encoding="utf-8")
            handles_l[ym] = open(os.path.join(base_l, f"{ym}.csv"), "w", newline="", encoding="utf-8")
            writers_a[ym] = csv.DictWriter(handles_a[ym], fieldnames=_A_FIELDNAMES)
            writers_l[ym] = csv.DictWriter(handles_l[ym], fieldnames=_LABELS_FIELDNAMES)
            writers_a[ym].writeheader()
            writers_l[ym].writeheader()
            counts[ym] = 0

        label3    = patient["_state"]
        eval_date = patient["evaluation_date"]
        age       = (eval_date - patient["dob"]).days // 365
        weight    = patient["base_weight"]
        bmi       = round(weight / (patient["height_cm"] / 100) ** 2, 2)
        fried     = {flag: _bernoulli(_FRIED_PROBS[flag][label3]) for flag in _FRIED_PROBS}
        updated   = _iso_ts(eval_date, random.randint(8, 17), random.randint(0, 59))
        label_date = patient["_label_available"].isoformat()

        writers_a[ym].writerow({
            "patient_id": patient["patient_id"], "snapshot_date": eval_date.isoformat(),
            "updated_at": updated, "age": age, "sex": patient["sex"],
            "height_cm": patient["height_cm"], "weight_kg": weight, "bmi": bmi,
            "heart_rate_bpm": int(_gauss_clamp(*_HR_PARAMS[label3])), **fried,
            "tug_time_s": round(_gauss_clamp(*_TUG_PARAMS[label3]), 2),
            "grip_strength_kg": round(_gauss_clamp(*_GRIP_PARAMS[label3]), 2),
            "mmse": int(_gauss_clamp(*_MMSE_PARAMS[label3])),
            "gds": round(_gauss_clamp(*_GDS_PARAMS[label3]), 1),
            "frailty_index_fi": round(_gauss_clamp(*_FI_PARAMS[label3]), 4),
        })
        writers_l[ym].writerow({
            "patient_id": patient["patient_id"], "snapshot_date": eval_date.isoformat(),
            "label_available_date": label_date, "frailty_label": patient["frailty_label"],
        })
        counts[ym] += 1

    for fh in list(handles_a.values()) + list(handles_l.values()):
        fh.close()
    for ym in sorted(counts):
        print(f"  Source A + Labels: {ym}.csv  ({counts[ym]:,} registros)")


def generate_source_b1(patients, output_dir):
    base = os.path.join(output_dir, "source_b1", "sppb_surveys")
    os.makedirs(base, exist_ok=True)
    handles, counts = {}, {}
    for patient in patients:
        ym = patient["_arrival_sppb"].strftime("%Y-%m")
        label3 = patient["_state"]
        if ym not in handles:
            handles[ym] = open(os.path.join(base, f"{ym}.jsonl"), "w", encoding="utf-8")
            counts[ym] = 0
        bal   = _weighted_choice(*_SPPB_BALANCE[label3])
        gait  = _weighted_choice(*_SPPB_GAIT[label3])
        chair = _weighted_choice(*_SPPB_CHAIR[label3])
        rec = {
            "response_id": f"SUR1_{patient['patient_id'][4:]}",
            "patient_id": patient["patient_id"],
            "survey_date": _iso_ts(patient["_arrival_sppb"], random.randint(8, 17), random.randint(0, 59)),
            "sppb_balance": bal, "sppb_gait_speed": gait, "sppb_chair_stand": chair,
            "sppb_total": bal + gait + chair,
            "fes_i_score": round(_gauss_clamp(*_FES_PARAMS[label3]), 1),
            "falls_last_year": _bernoulli(_FALLS_PROBS[label3]),
        }
        handles[ym].write(json.dumps(rec) + "\n")
        counts[ym] += 1
    for fh in handles.values():
        fh.close()
    for ym in sorted(counts):
        print(f"  Source B1: {ym}.jsonl  ({counts[ym]:,} registros)")


def generate_source_b2(patients, output_dir):
    base = os.path.join(output_dir, "source_b2", "lifestyle_surveys")
    os.makedirs(base, exist_ok=True)
    handles, counts = {}, {}
    for patient in patients:
        ym = patient["_arrival_lifestyle"].strftime("%Y-%m")
        label3 = patient["_state"]
        if ym not in handles:
            handles[ym] = open(os.path.join(base, f"{ym}.jsonl"), "w", encoding="utf-8")
            counts[ym] = 0
        htn = _bernoulli(_HTN_P[label3])
        dia = _bernoulli(_DIA_P[label3])
        art = _bernoulli(_ART_P[label3])
        rec = {
            "response_id": f"SUR2_{patient['patient_id'][4:]}",
            "patient_id": patient["patient_id"],
            "survey_date": _iso_ts(patient["_arrival_lifestyle"], random.randint(8, 17), random.randint(0, 59)),
            "physical_activity_vigorous": int(round(_gauss_clamp(*_PA_VIG_PARAMS[label3]))),
            "physical_activity_moderate": int(round(_gauss_clamp(*_PA_MOD_PARAMS[label3]))),
            "sedentary_hours_day": round(_gauss_clamp(*_SED_PARAMS[label3]), 1),
            "depression": _bernoulli(_DEPR_P[label3]),
            "hypertension": htn, "diabetes": dia, "arthritis": art,
            "num_chronic_conditions": htn + dia + art + random.randint(0, 2),
        }
        handles[ym].write(json.dumps(rec) + "\n")
        counts[ym] += 1
    for fh in handles.values():
        fh.close()
    for ym in sorted(counts):
        print(f"  Source B2: {ym}.jsonl  ({counts[ym]:,} registros)")


def generate_source_c(patients, output_dir):
    base = os.path.join(output_dir, "source_c", "gait_events")
    os.makedirs(base, exist_ok=True)
    handles, counts = {}, {}
    for patient in patients:
        ym = patient["_arrival_gait"].strftime("%Y-%m")
        label3 = patient["_state"]
        pid_hash = patient["patient_id"][4:]
        if ym not in handles:
            handles[ym] = open(os.path.join(base, f"{ym}.jsonl"), "w", encoding="utf-8")
            counts[ym] = 0

        session_date = patient["evaluation_date"] + timedelta(days=patient["session_jitter"])
        mu_dur = _gauss_clamp(*_STRIDE_DUR_BASE[label3])
        mu_len = _gauss_clamp(*_STRIDE_LEN_BASE[label3])
        mu_foot = _gauss_clamp(*_FOOT_CLEAR_BASE[label3])
        mu_toe  = _gauss_clamp(*_TOE_OFF_BASE[label3])
        mu_heel = _gauss_clamp(*_HEEL_STRIKE_BASE[label3])
        mu_lat  = _gauss_clamp(*_LATERAL_BASE[label3])
        sigma_intra = _gauss_clamp(*_SIGMA_INTRA_DUR[label3])
        n_strides = STRIDES_PER_SESSION + random.randint(-5, 5)
        session_id = f"SES_{pid_hash}"
        sess_hour = random.randint(8, 18)
        sess_min  = random.randint(0, 59)
        session_ts = _iso_ts(session_date, sess_hour, sess_min)
        base_secs  = sess_hour * 3600 + sess_min * 60
        offset_s   = 0.0

        fh = handles[ym]
        for s in range(n_strides):
            dur  = _gauss_clamp(mu_dur, sigma_intra, _STRIDE_DUR_BASE[label3][2], _STRIDE_DUR_BASE[label3][3])
            slen = _gauss_clamp(mu_len, 0.05, _STRIDE_LEN_BASE[label3][2], _STRIDE_LEN_BASE[label3][3])
            foot = _gauss_clamp(mu_foot, 0.010, _FOOT_CLEAR_BASE[label3][2], _FOOT_CLEAR_BASE[label3][3])
            toe  = _gauss_clamp(mu_toe, 1.5, _TOE_OFF_BASE[label3][2], _TOE_OFF_BASE[label3][3])
            heel = _gauss_clamp(mu_heel, 1.5, _HEEL_STRIKE_BASE[label3][2], _HEEL_STRIKE_BASE[label3][3])
            lat  = _gauss_clamp(mu_lat, 0.010, _LATERAL_BASE[label3][2], _LATERAL_BASE[label3][3])
            swing_prop = _gauss_clamp(*_SWING_PROP[label3])
            swing = round(dur * swing_prop, 4)
            abs_s = min(int(base_secs + offset_s), 86399)
            ts_h, rem = divmod(abs_s, 3600)
            ts_m, ts_s = divmod(rem, 60)
            fh.write(json.dumps({
                "event_id": f"GAI_{pid_hash}_{s:06d}",
                "patient_id": patient["patient_id"],
                "session_id": session_id, "session_timestamp": session_ts,
                "stride_timestamp": _iso_ts(session_date, ts_h, ts_m, ts_s),
                "stride_index": s,
                "stride_duration_s": round(dur, 4), "stride_length_m": round(slen, 4),
                "swing_time_s": swing, "stance_time_s": round(dur - swing, 4),
                "foot_clearance_m": round(foot, 4), "toe_off_angle_deg": round(toe, 2),
                "heel_strike_angle_deg": round(heel, 2), "lateral_excursion_m": round(lat, 4),
            }) + "\n")
            counts[ym] += 1
            offset_s += dur + random.randint(0, 50) / 1000.0

    for fh in handles.values():
        fh.close()
    total = sum(counts.values())
    for ym in sorted(counts):
        print(f"  Source C: {ym}.jsonl  ({counts[ym]:,} zancadas)")
    print(f"  Source C TOTAL: {total:,} zancadas")


def main():
    parser = argparse.ArgumentParser(description="Genera un lote adicional de pacientes sintéticos")
    parser.add_argument("--n",          type=int,  required=True,  help="Número de pacientes")
    parser.add_argument("--seed",       type=int,  required=True,  help="Semilla aleatoria")
    parser.add_argument("--eval-start", required=True, help="Inicio de ventana de evaluación YYYY-MM-DD")
    parser.add_argument("--eval-end",   required=True, help="Fin de ventana de evaluación YYYY-MM-DD")
    parser.add_argument("--output-dir", required=True, help="Directorio de salida")
    args = parser.parse_args()

    eval_start = date.fromisoformat(args.eval_start)
    eval_end   = date.fromisoformat(args.eval_end)

    print(f"Generando {args.n:,} pacientes  seed={args.seed}  [{eval_start} — {eval_end}]")
    print(f"Salida: {args.output_dir}/")

    patients = build_population(args.n, args.seed, eval_start, eval_end)
    lc = {0: sum(1 for p in patients if p["frailty_label"] == 0),
          1: sum(1 for p in patients if p["frailty_label"] == 1)}
    print(f"  No frágil={lc[0]:,} ({lc[0]/args.n:.1%})  Frágil={lc[1]:,} ({lc[1]/args.n:.1%})")

    print("\nGenerando Fuente A + Labels...")
    generate_source_a(patients, args.output_dir)
    print("\nGenerando Fuente B1 (SPPB)...")
    generate_source_b1(patients, args.output_dir)
    print("\nGenerando Fuente B2 (Lifestyle)...")
    generate_source_b2(patients, args.output_dir)
    print("\nGenerando Fuente C (Gait)...")
    generate_source_c(patients, args.output_dir)
    print("\nHecho.")


if __name__ == "__main__":
    main()
