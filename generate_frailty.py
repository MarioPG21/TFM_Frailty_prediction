"""
Generador de datos sintéticos para estimación de fragilidad
==========================================================
Genera datos longitudinales multimodales para estimación del nivel de fragilidad
en personas mayores. Ver generador_datos_sinteticos.md para decisiones de diseño.

Salida:
  synthetic_data/
    source_a/clinical_records/YYYY-MM.csv          (mensual)
    source_b1/sppb_surveys/YYYY-MM.jsonl           (semestral: jun, dic, jun)
    source_b2/lifestyle_surveys/YYYY-MM.jsonl      (semestral: jun, dic, jun)
    source_c/gait_events/YYYY-MM.jsonl             (mensual, ~26 sesiones/paciente)

Reproducibilidad: módulo random de stdlib con SEED=42. Sin NumPy ni dependencias externas.
"""

import csv
import hashlib
import json
import os
import random
from datetime import date, timedelta

# =============================================================================
# 1. CONSTANTES GLOBALES Y SEMILLA
# =============================================================================

SEED                = 42
random.seed(SEED)

N_PATIENTS          = 10_000
WINDOW_START        = date(2024, 1, 1)
WINDOW_END          = date(2025, 6, 30)
ENROLLMENT_CUTOFF   = date(2024, 6, 30)
SESSIONS_PER_18M    = 26
STRIDES_PER_SESSION = 40
OUTPUT_DIR          = "synthetic_data"

# =============================================================================
# 2. FUNCIONES AUXILIARES DE MUESTREO
# =============================================================================

def gauss_clamp(mu, sigma, lo, hi):
    """Gaussiana truncada. Reintenta hasta obtener valor en [lo, hi]. Máximo 200 intentos."""
    for _ in range(200):
        v = random.gauss(mu, sigma)
        if lo <= v <= hi:
            return v
    return max(lo, min(hi, random.gauss(mu, sigma)))


def weighted_choice(options, weights):
    """Muestreo ponderado sin numpy. Los pesos no necesitan sumar 1."""
    total = sum(weights)
    r = random.random() * total
    cum = 0.0
    for opt, w in zip(options, weights):
        cum += w
        if r <= cum:
            return opt
    return options[-1]


def bernoulli(p):
    return 1 if random.random() < p else 0


def make_id(prefix, index):
    """Identificador reproducible: hash MD5 truncado del índice y la semilla."""
    h = hashlib.md5(f"{prefix}{index}{SEED}".encode()).hexdigest()[:8].upper()
    return f"{prefix}_{h}"


def iso_ts(d, hour, minute, second=0):
    """Convierte fecha + hora a timestamp ISO 8601."""
    return f"{d.isoformat()}T{hour:02d}:{minute:02d}:{second:02d}Z"


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def clamp_date(d, lo, hi):
    if d < lo: return lo
    if d > hi: return hi
    return d


def days_in_month(year, month):
    if month == 12:
        return (date(year + 1, 1, 1) - date(year, month, 1)).days
    return (date(year, month + 1, 1) - date(year, month, 1)).days


# =============================================================================
# 3. GENERADOR DE LA POBLACIÓN BASE
# =============================================================================

_AGE_PARAMS    = {0: (72,5,65,85),   1: (77,6,65,90),   2: (82,6,65,95)}
_HEIGHT_PARAMS = {0: (167,9,145,190), 1: (166,9,145,190), 2: (164,9,145,185)}
_WEIGHT_PARAMS = {0: (72,12,45,105), 1: (68,13,42,105), 2: (63,13,40,100)}


def effective_label3(patient, months_since_enrollment):
    """Estado interno (0/1/2) efectivo en un mes dado, aplicando transición si procede.
    Solo se usa para condicionar las distribuciones de generación; nunca se expone en disco."""
    if patient["transition_month"] is not None:
        if months_since_enrollment >= patient["transition_month"]:
            return min(2, patient["_state"] + 1)
    return patient["_state"]


def effective_frailty_label(patient, months_since_enrollment):
    """Etiqueta binaria efectiva (0=no frágil, 1=frágil) en un mes dado."""
    return 0 if effective_label3(patient, months_since_enrollment) < 2 else 1


def build_population():
    """
    Construye la lista de 10.000 pacientes.
    Es la fuente de verdad de la que se alimentan todos los generadores.
    La etiqueta se asigna primero; todas las variables se derivan condicionadas a ella.
    """
    enrollment_span = (ENROLLMENT_CUTOFF - WINDOW_START).days
    patients = []

    for i in range(N_PATIENTS):
        label3 = weighted_choice([0, 1, 2], [0.40, 0.35, 0.25])  # estado interno: 0=robusto,1=prefrágil,2=frágil
        sex    = weighted_choice(["M", "F"], [0.45, 0.55])

        age_at_start = int(gauss_clamp(*_AGE_PARAMS[label3]))
        dob = date(WINDOW_START.year - age_at_start, 7, 1)

        height = round(gauss_clamp(*_HEIGHT_PARAMS[label3]), 1)
        weight = round(gauss_clamp(*_WEIGHT_PARAMS[label3]), 1)

        enrollment_date = WINDOW_START + timedelta(days=random.randint(0, enrollment_span))

        # Transición unidireccional de estado durante los 18 meses
        transition_month = None
        if label3 == 1 and random.random() < 0.12:    # prefrágil → frágil
            transition_month = random.randint(3, 17)
        elif label3 == 0 and random.random() < 0.06:  # robusto → prefrágil
            transition_month = random.randint(3, 17)

        # Jitters de sesiones de marcha precalculados para mantener el orden del estado random
        session_jitters = [random.randint(-3, 3) for _ in range(SESSIONS_PER_18M)]

        patients.append({
            "patient_id":       make_id("PAT", i),
            "sex":              sex,
            "dob":              dob,
            "_state":           label3,           # estado interno (0/1/2), nunca se escribe en disco
            "frailty_label":    0 if label3 < 2 else 1,   # etiqueta de salida binaria
            "height_cm":        height,
            "base_weight":      weight,
            "enrollment_date":  enrollment_date,
            "transition_month": transition_month,
            "session_jitters":  session_jitters,
        })

    return patients


# =============================================================================
# 4. GENERADOR DE LA FUENTE A (REGISTROS CLÍNICOS ESTRUCTURADOS, CSV MENSUAL)
# =============================================================================

_HR_PARAMS   = {0: (68,8,50,90),    1: (72,9,50,95),    2: (76,10,50,100)}
_TUG_PARAMS  = {0: (9,2,5,14),      1: (13,3,8,22),     2: (20,6,10,60)}
_GRIP_PARAMS = {0: (32,6,18,50),    1: (24,5,12,38),    2: (16,5,5,28)}
_MMSE_PARAMS = {0: (28,1.5,24,30),  1: (26,2,20,30),    2: (23,3,10,29)}
_GDS_PARAMS  = {0: (1.5,0.5,1,3),   1: (2.5,1,1,5),     2: (4,1.5,1,7)}
_FI_PARAMS   = {0: (0.10,0.04,0.0,0.20), 1: (0.22,0.04,0.10,0.35), 2: (0.38,0.07,0.25,0.70)}

_FRIED_PROBS = {
    "fried_weight_loss":  {0: 0.05, 1: 0.20, 2: 0.55},
    "fried_weakness":     {0: 0.08, 1: 0.35, 2: 0.75},
    "fried_slowness":     {0: 0.06, 1: 0.30, 2: 0.70},
    "fried_low_activity": {0: 0.15, 1: 0.45, 2: 0.80},
    "fried_exhaustion":   {0: 0.10, 1: 0.35, 2: 0.65},
}

_A_FIELDNAMES = [
    "patient_id", "snapshot_date", "updated_at", "age", "sex",
    "height_cm", "weight_kg", "bmi", "heart_rate_bpm",
    "fried_weight_loss", "fried_weakness", "fried_slowness",
    "fried_low_activity", "fried_exhaustion",
    "tug_time_s", "grip_strength_kg", "mmse", "gds",
    "frailty_index_fi", "frailty_label",
]


def _vary(prev_val, lo, hi):
    """Variación de hasta ±5% sobre el valor previo, respetando límites de rango."""
    delta = prev_val * gauss_clamp(0, 0.025, -0.05, 0.05)
    return clamp(prev_val + delta, lo, hi)


def _clinical_snapshot(patient, snapshot_date, months_since, prev):
    label3 = effective_label3(patient, months_since)         # estado interno (no se expone)
    age    = (snapshot_date - patient["dob"]).days // 365

    dim        = days_in_month(snapshot_date.year, snapshot_date.month)
    updated_at = iso_ts(
        date(snapshot_date.year, snapshot_date.month, random.randint(1, dim)),
        random.randint(8, 17), random.randint(0, 59)
    )

    if prev is None:
        hr     = int(gauss_clamp(*_HR_PARAMS[label3]))
        tug    = round(gauss_clamp(*_TUG_PARAMS[label3]), 2)
        grip   = round(gauss_clamp(*_GRIP_PARAMS[label3]), 2)
        mmse   = int(gauss_clamp(*_MMSE_PARAMS[label3]))
        gds    = round(gauss_clamp(*_GDS_PARAMS[label3]), 1)
        fi     = round(gauss_clamp(*_FI_PARAMS[label3]), 4)
        weight = patient["base_weight"]
    else:
        hr     = int(clamp(prev["heart_rate_bpm"] + random.gauss(0, 2),
                           _HR_PARAMS[label3][2], _HR_PARAMS[label3][3]))
        tug    = round(_vary(prev["tug_time_s"],        _TUG_PARAMS[label3][2],  _TUG_PARAMS[label3][3]),  2)
        grip   = round(_vary(prev["grip_strength_kg"],  _GRIP_PARAMS[label3][2], _GRIP_PARAMS[label3][3]), 2)
        mmse   = int(clamp(prev["mmse"] + random.randint(-1, 1),
                           _MMSE_PARAMS[label3][2], _MMSE_PARAMS[label3][3]))
        gds    = round(clamp(prev["gds"] + random.gauss(0, 0.2),
                             _GDS_PARAMS[label3][2], _GDS_PARAMS[label3][3]), 1)
        fi     = round(_vary(prev["frailty_index_fi"],  _FI_PARAMS[label3][2],   _FI_PARAMS[label3][3]),  4)
        weight = round(_vary(prev["weight_kg"], 40, 105), 1)

    bmi   = round(weight / (patient["height_cm"] / 100) ** 2, 2)
    fried = {flag: bernoulli(_FRIED_PROBS[flag][label3]) for flag in _FRIED_PROBS}

    return {
        "patient_id":       patient["patient_id"],
        "snapshot_date":    snapshot_date.isoformat(),
        "updated_at":       updated_at,
        "age":              age,
        "sex":              patient["sex"],
        "height_cm":        patient["height_cm"],
        "weight_kg":        weight,
        "bmi":              bmi,
        "heart_rate_bpm":   hr,
        **fried,
        "tug_time_s":       tug,
        "grip_strength_kg": grip,
        "mmse":             mmse,
        "gds":              gds,
        "frailty_index_fi": fi,
        "frailty_label":    0 if label3 < 2 else 1,
    }


def generate_source_a(patients, output_dir):
    base = os.path.join(output_dir, "source_a", "clinical_records")
    os.makedirs(base, exist_ok=True)

    prev = {}  # patient_id → snapshot del mes anterior (para variación continua)

    for mo in range(18):
        year  = 2024 + mo // 12
        month = mo % 12 + 1
        snapshot_date = date(year, month, 1)
        fpath = os.path.join(base, f"{year}-{month:02d}.csv")

        rows = []
        for patient in patients:
            if patient["enrollment_date"] > snapshot_date:
                continue
            months_since = (
                (snapshot_date.year  - patient["enrollment_date"].year)  * 12 +
                (snapshot_date.month - patient["enrollment_date"].month)
            )
            pid = patient["patient_id"]
            rec = _clinical_snapshot(patient, snapshot_date, months_since, prev.get(pid))
            prev[pid] = rec
            rows.append(rec)

        with open(fpath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_A_FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)

        print(f"  Source A: {year}-{month:02d}.csv  ({len(rows):,} registros)")


# =============================================================================
# 5. GENERADOR DE LA FUENTE B1 (ENCUESTA SPPB Y MIEDO A CAÍDAS, JSONL SEMESTRAL)
# =============================================================================

_SPPB_BALANCE = {
    0: ([3, 4],       [0.3, 0.7]),
    1: ([2, 3, 4],    [0.3, 0.5, 0.2]),
    2: ([0, 1, 2, 3], [0.2, 0.3, 0.3, 0.2]),
}
_SPPB_GAIT = {
    0: ([3, 4],    [0.4, 0.6]),
    1: ([2, 3],    [0.5, 0.5]),
    2: ([0, 1, 2], [0.3, 0.4, 0.3]),
}
_SPPB_CHAIR = {
    0: ([3, 4],    [0.4, 0.6]),
    1: ([1, 2, 3], [0.3, 0.4, 0.3]),
    2: ([0, 1, 2], [0.4, 0.4, 0.2]),
}
_FES_PARAMS  = {0: (10,2,7,16),   1: (16,3,9,24),   2: (22,3,14,28)}
_FALLS_PROBS = {0: 0.10, 1: 0.28, 2: 0.52}

_B1_WAVES = [
    (date(2024, 6, 15),  "2024-06"),
    (date(2024, 12, 15), "2024-12"),
    (date(2025, 6, 15),  "2025-06"),
]


def generate_source_b1(patients, output_dir):
    base = os.path.join(output_dir, "source_b1", "sppb_surveys")
    os.makedirs(base, exist_ok=True)

    for wave_center, wave_label in _B1_WAVES:
        wy, wm = int(wave_label[:4]), int(wave_label[5:])
        fpath  = os.path.join(base, f"{wave_label}.jsonl")
        count  = 0

        with open(fpath, "w", encoding="utf-8") as f:
            for patient in patients:
                if patient["enrollment_date"] > wave_center:
                    continue
                months_since = (
                    (wy - patient["enrollment_date"].year)  * 12 +
                    (wm - patient["enrollment_date"].month)
                )
                label3 = effective_label3(patient, months_since)

                jitter   = random.randint(-15, 15)
                survey_d = clamp_date(wave_center + timedelta(days=jitter),
                                      date(wy, wm, 1), date(wy, wm, 28))
                survey_ts = iso_ts(survey_d, random.randint(8, 17), random.randint(0, 59))

                bal   = weighted_choice(*_SPPB_BALANCE[label3])
                gait  = weighted_choice(*_SPPB_GAIT[label3])
                chair = weighted_choice(*_SPPB_CHAIR[label3])

                rec = {
                    "response_id":      f"SUR1_{patient['patient_id'][4:]}_{wave_label}",
                    "patient_id":       patient["patient_id"],
                    "survey_date":      survey_ts,
                    "sppb_balance":     bal,
                    "sppb_gait_speed":  gait,
                    "sppb_chair_stand": chair,
                    "sppb_total":       bal + gait + chair,
                    "fes_i_score":      round(gauss_clamp(*_FES_PARAMS[label3]), 1),
                    "falls_last_year":  bernoulli(_FALLS_PROBS[label3]),
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                count += 1

        print(f"  Source B1: {wave_label}.jsonl  ({count:,} registros)")


# =============================================================================
# 6. GENERADOR DE LA FUENTE B2 (ENCUESTA HÁBITOS Y CONDICIONES CRÓNICAS, JSONL SEMESTRAL)
# =============================================================================

_PA_VIG_PARAMS = {0: (2,1,0,7),     1: (1,1,0,5),     2: (0.3,0.5,0,3)}
_PA_MOD_PARAMS = {0: (4,1.5,0,7),   1: (2.5,1.5,0,7), 2: (1,1,0,5)}
_SED_PARAMS    = {0: (5,2,1,10),    1: (8,2.5,2,14),  2: (11,2.5,4,18)}
_DEPR_P        = {0: 0.08, 1: 0.22, 2: 0.45}
_HTN_P         = {0: 0.30, 1: 0.50, 2: 0.65}
_DIA_P         = {0: 0.12, 1: 0.22, 2: 0.35}
_ART_P         = {0: 0.15, 1: 0.35, 2: 0.60}

_B2_WAVES = [
    (date(2024, 6, 15),  "2024-06"),
    (date(2024, 12, 15), "2024-12"),
    (date(2025, 6, 15),  "2025-06"),
]


def generate_source_b2(patients, output_dir):
    base = os.path.join(output_dir, "source_b2", "lifestyle_surveys")
    os.makedirs(base, exist_ok=True)

    for wave_center, wave_label in _B2_WAVES:
        wy, wm = int(wave_label[:4]), int(wave_label[5:])
        fpath  = os.path.join(base, f"{wave_label}.jsonl")
        count  = 0

        with open(fpath, "w", encoding="utf-8") as f:
            for patient in patients:
                if patient["enrollment_date"] > wave_center:
                    continue
                months_since = (
                    (wy - patient["enrollment_date"].year)  * 12 +
                    (wm - patient["enrollment_date"].month)
                )
                label3 = effective_label3(patient, months_since)

                # Jitter independiente del de B1
                jitter   = random.randint(-15, 15)
                survey_d = clamp_date(wave_center + timedelta(days=jitter),
                                      date(wy, wm, 1), date(wy, wm, 28))
                survey_ts = iso_ts(survey_d, random.randint(8, 17), random.randint(0, 59))

                pa_vig = int(round(gauss_clamp(*_PA_VIG_PARAMS[label3])))
                pa_mod = int(round(gauss_clamp(*_PA_MOD_PARAMS[label3])))
                sed    = round(gauss_clamp(*_SED_PARAMS[label3]), 1)
                dep    = bernoulli(_DEPR_P[label3])
                htn    = bernoulli(_HTN_P[label3])
                dia    = bernoulli(_DIA_P[label3])
                art    = bernoulli(_ART_P[label3])
                extra  = random.randint(0, 2)

                rec = {
                    "response_id":                f"SUR2_{patient['patient_id'][4:]}_{wave_label}",
                    "patient_id":                 patient["patient_id"],
                    "survey_date":                survey_ts,
                    "physical_activity_vigorous": pa_vig,
                    "physical_activity_moderate": pa_mod,
                    "sedentary_hours_day":         sed,
                    "depression":                 dep,
                    "hypertension":               htn,
                    "diabetes":                   dia,
                    "arthritis":                  art,
                    "num_chronic_conditions":     htn + dia + art + extra,
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                count += 1

        print(f"  Source B2: {wave_label}.jsonl  ({count:,} registros)")


# =============================================================================
# 7. GENERADOR DE LA FUENTE C (EVENTOS DE ZANCADA, JSONL MENSUAL)
# =============================================================================

# Parámetros base de sesión por estado (mu, sigma, lo, hi) — variabilidad inter-sesión
_STRIDE_DUR_BASE  = {0: (1.05,0.08,0.85,1.25), 1: (1.25,0.10,1.00,1.60), 2: (1.55,0.15,1.20,2.50)}
_STRIDE_LEN_BASE  = {0: (1.10,0.12,0.80,1.45), 1: (0.85,0.12,0.55,1.15), 2: (0.60,0.12,0.20,0.90)}
_FOOT_CLEAR_BASE  = {0: (0.12,0.02,0.06,0.18), 1: (0.08,0.02,0.04,0.14), 2: (0.05,0.02,0.01,0.10)}
_TOE_OFF_BASE     = {0: (-16,3,-25,-5),          1: (-10,4,-20,0),          2: (-5,4,-15,5)}
_HEEL_STRIKE_BASE = {0: (10,3,2,18),             1: (6,3,-2,14),            2: (2,4,-10,12)}
_LATERAL_BASE     = {0: (0.04,0.01,0.01,0.07),  1: (0.07,0.02,0.03,0.12), 2: (0.11,0.03,0.05,0.20)}

# Sigma intra-sesión de stride_duration_s — determina el stride_time_cv en Gold
_SIGMA_INTRA_DUR  = {0: (0.030,0.008,0.010,0.060), 1: (0.060,0.015,0.025,0.110), 2: (0.110,0.025,0.050,0.220)}

# Proporción swing/stride por estado
_SWING_PROP       = {0: (0.38,0.02,0.32,0.44), 1: (0.34,0.02,0.28,0.40), 2: (0.30,0.03,0.24,0.38)}


def _generate_strides(patient, session_date, sess_i, months_since):
    """Genera las zancadas de una sesión de marcha."""
    label3 = effective_label3(patient, months_since)
    pid_hash = patient["patient_id"][4:]  # sin "PAT_"

    # Parámetros base de esta sesión (variabilidad inter-sesión: mu del día)
    mu_dur  = gauss_clamp(*_STRIDE_DUR_BASE[label3])
    mu_len  = gauss_clamp(*_STRIDE_LEN_BASE[label3])
    mu_foot = gauss_clamp(*_FOOT_CLEAR_BASE[label3])
    mu_toe  = gauss_clamp(*_TOE_OFF_BASE[label3])
    mu_heel = gauss_clamp(*_HEEL_STRIKE_BASE[label3])
    mu_lat  = gauss_clamp(*_LATERAL_BASE[label3])
    sigma_intra = gauss_clamp(*_SIGMA_INTRA_DUR[label3])

    n_strides  = STRIDES_PER_SESSION + random.randint(-5, 5)
    session_id = f"SES_{pid_hash}_{session_date.isoformat()}"
    sess_hour  = random.randint(8, 18)
    sess_min   = random.randint(0, 59)
    session_ts = iso_ts(session_date, sess_hour, sess_min)
    base_secs  = sess_hour * 3600 + sess_min * 60

    strides  = []
    offset_s = 0.0  # segundos desde el inicio de sesión

    for s in range(n_strides):
        dur  = gauss_clamp(mu_dur,  sigma_intra, _STRIDE_DUR_BASE[label3][2],  _STRIDE_DUR_BASE[label3][3])
        slen = gauss_clamp(mu_len,  0.05,         _STRIDE_LEN_BASE[label3][2],  _STRIDE_LEN_BASE[label3][3])
        foot = gauss_clamp(mu_foot, 0.010,        _FOOT_CLEAR_BASE[label3][2],  _FOOT_CLEAR_BASE[label3][3])
        toe  = gauss_clamp(mu_toe,  1.5,          _TOE_OFF_BASE[label3][2],     _TOE_OFF_BASE[label3][3])
        heel = gauss_clamp(mu_heel, 1.5,          _HEEL_STRIKE_BASE[label3][2], _HEEL_STRIKE_BASE[label3][3])
        lat  = gauss_clamp(mu_lat,  0.010,        _LATERAL_BASE[label3][2],     _LATERAL_BASE[label3][3])
        swing_prop = gauss_clamp(*_SWING_PROP[label3])
        swing = round(dur * swing_prop, 4)

        # Timestamp de esta zancada: inicio de sesión + offset acumulado
        abs_s = int(base_secs + offset_s)
        abs_s = min(abs_s, 86399)
        ts_h, rem = divmod(abs_s, 3600)
        ts_m, ts_s = divmod(rem, 60)
        stride_ts = iso_ts(session_date, ts_h, ts_m, ts_s)

        # event_id único por paciente (índice global = sesión × 1000 + zancada)
        event_id = f"GAI_{pid_hash}_{sess_i * 1000 + s:06d}"

        strides.append({
            "event_id":              event_id,
            "patient_id":            patient["patient_id"],
            "session_id":            session_id,
            "session_timestamp":     session_ts,
            "stride_index":          s,
            "stride_duration_s":     round(dur,  4),
            "stride_length_m":       round(slen, 4),
            "swing_time_s":          swing,
            "stance_time_s":         round(dur - swing, 4),
            "foot_clearance_m":      round(foot, 4),
            "toe_off_angle_deg":     round(toe,  2),
            "heel_strike_angle_deg": round(heel, 2),
            "lateral_excursion_m":   round(lat,  4),
        })

        # Avanzar tiempo: duración de la zancada + jitter 0-50 ms
        offset_s += dur + random.randint(0, 50) / 1000.0

    return strides


def generate_source_c(patients, output_dir):
    """
    Genera los ficheros mensuales de eventos de zancada.
    Escribe directamente a disco sesión a sesión para minimizar uso de memoria.
    """
    base = os.path.join(output_dir, "source_c", "gait_events")
    os.makedirs(base, exist_ok=True)

    # Abrir los 18 ficheros mensuales simultáneamente
    handles = {}
    counts  = {}
    for mo in range(18):
        year  = 2024 + mo // 12
        month = mo % 12 + 1
        key   = (year, month)
        handles[key] = open(os.path.join(base, f"{year}-{month:02d}.jsonl"), "w", encoding="utf-8")
        counts[key]  = 0

    for idx, patient in enumerate(patients):
        if idx % 2000 == 0:
            print(f"    {idx:,}/{N_PATIENTS:,} pacientes...")
        enroll = patient["enrollment_date"]
        for sess_i, jitter in enumerate(patient["session_jitters"]):
            session_date = enroll + timedelta(days=sess_i * 14 + jitter)
            if session_date < WINDOW_START or session_date > WINDOW_END:
                continue
            key = (session_date.year, session_date.month)
            if key not in handles:
                continue
            months_since = max(0,
                (session_date.year  - enroll.year)  * 12 +
                (session_date.month - enroll.month))
            strides = _generate_strides(patient, session_date, sess_i, months_since)
            fh = handles[key]
            for st in strides:
                fh.write(json.dumps(st, ensure_ascii=False) + "\n")
            counts[key] += len(strides)

    for fh in handles.values():
        fh.close()

    total = sum(counts.values())
    for mo in range(18):
        year  = 2024 + mo // 12
        month = mo % 12 + 1
        key   = (year, month)
        print(f"  Source C: {year}-{month:02d}.jsonl  ({counts[key]:,} zancadas)")
    print(f"  Source C: TOTAL {total:,} zancadas")
    return total


# =============================================================================
# 8. VERIFICACIONES (CONTRATO DE SALIDA)
# =============================================================================

def _rank(values):
    n = len(values)
    order = sorted(range(n), key=lambda i: values[i])
    ranks = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j < n - 1 and values[order[j]] == values[order[j + 1]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def _spearman(x, y):
    rx, ry = _rank(x), _rank(y)
    n  = len(rx)
    mx = sum(rx) / n
    my = sum(ry) / n
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    sx  = sum((a - mx) ** 2 for a in rx) ** 0.5
    sy  = sum((b - my) ** 2 for b in ry) ** 0.5
    if sx == 0 or sy == 0:
        return 0.0
    return num / (sx * sy)


def run_verifications(patients, output_dir):
    print("\n" + "=" * 60)
    print("VERIFICACIONES")
    print("=" * 60)
    all_ok = True

    # V1: Distribución binaria (tolerancia ±3 pp): no frágil ~75%, frágil ~25%
    counts  = {0: 0, 1: 0}
    for p in patients:
        counts[p["frailty_label"]] += 1
    targets = {0: 0.75, 1: 0.25}
    nombres = {0: "No frágil", 1: "Frágil"}
    for lbl, target in targets.items():
        actual = counts[lbl] / N_PATIENTS
        diff   = abs(actual - target)
        ok     = diff <= 0.03
        all_ok &= ok
        print(f"  V1 {nombres[lbl]}: objetivo={target:.0%}  real={actual:.2%}  "
              f"desviación={diff:.2%}  [{'OK' if ok else 'FAIL'}]")

    # V2: Volumen Fuente C (admite jitter ±5 zancadas/sesión)
    c_base  = os.path.join(output_dir, "source_c", "gait_events")
    total_c = 0
    for fname in os.listdir(c_base):
        with open(os.path.join(c_base, fname), encoding="utf-8") as f:
            total_c += sum(1 for _ in f)
    lo_v2 = N_PATIENTS * SESSIONS_PER_18M * (STRIDES_PER_SESSION - 5)
    hi_v2 = N_PATIENTS * SESSIONS_PER_18M * (STRIDES_PER_SESSION + 5)
    ok = lo_v2 <= total_c <= hi_v2
    all_ok &= ok
    print(f"  V2 Fuente C volumen: {total_c:,} zancadas  "
          f"(esperado {lo_v2:,}–{hi_v2:,})  [{'OK' if ok else 'FAIL'}]")

    # V3: Sin valores fuera de rango (último CSV de Fuente A)
    a_base   = os.path.join(output_dir, "source_a", "clinical_records")
    last_csv = sorted(os.listdir(a_base))[-1]
    range_ok = True
    range_fail_msg = ""
    checks = [("tug_time_s", 5.0, 60.0), ("grip_strength_kg", 5.0, 50.0),
              ("frailty_index_fi", 0.0, 0.70), ("age", 65, 95)]
    with open(os.path.join(a_base, last_csv), encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for col, lo, hi in checks:
                v = float(row[col])
                if not (lo <= v <= hi):
                    range_ok = False
                    range_fail_msg = f"{col}={v} fuera de [{lo},{hi}]"
                    break
            if not range_ok:
                break
    all_ok &= range_ok
    msg = range_fail_msg if not range_ok else ""
    print(f"  V3 Rangos numéricos ({last_csv}):  [{'OK' if range_ok else 'FAIL ' + msg}]")

    # V4: Correlaciones de Spearman sobre último snapshot (etiqueta binaria)
    labels, tugs, grips, fis = [], [], [], []
    with open(os.path.join(a_base, last_csv), encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            labels.append(int(row["frailty_label"]))
            tugs.append(float(row["tug_time_s"]))
            grips.append(float(row["grip_strength_kg"]))
            fis.append(float(row["frailty_index_fi"]))

    r_tug  = _spearman(labels, tugs)
    r_grip = _spearman(labels, grips)
    r_fi   = _spearman(labels, fis)
    ok_tug  = r_tug  >  0.40
    ok_grip = r_grip < -0.40
    ok_fi   = r_fi   >  0.50
    all_ok &= ok_tug and ok_grip and ok_fi
    print(f"  V4 Spearman(label, tug):   {r_tug:+.3f}  (>+0.40)  [{'OK' if ok_tug  else 'FAIL'}]")
    print(f"  V4 Spearman(label, grip):  {r_grip:+.3f}  (<-0.40)  [{'OK' if ok_grip else 'FAIL'}]")
    print(f"  V4 Spearman(label, fi):    {r_fi:+.3f}  (>+0.50)  [{'OK' if ok_fi   else 'FAIL'}]")

    # V5: Integridad referencial (Fuentes B1, B2, C — muestra de primera ola)
    patient_ids = {p["patient_id"] for p in patients}
    orphans = 0
    for sub in ["source_b1/sppb_surveys", "source_b2/lifestyle_surveys"]:
        sub_base = os.path.join(output_dir, sub)
        for fname in sorted(os.listdir(sub_base))[:1]:  # primera ola como muestra
            with open(os.path.join(sub_base, fname), encoding="utf-8") as f:
                for line in f:
                    if json.loads(line)["patient_id"] not in patient_ids:
                        orphans += 1
    ok = orphans == 0
    all_ok &= ok
    print(f"  V5 Integridad referencial B1/B2: registros huérfanos={orphans}  [{'OK' if ok else 'FAIL'}]")

    print()
    status = "TODAS LAS VERIFICACIONES PASADAS" if all_ok else "HAY VERIFICACIONES FALLIDAS"
    print(f"  RESULTADO GLOBAL: {status}")
    return all_ok


# =============================================================================
# 9. MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("Generador de datos sintéticos — Estimación de fragilidad")
    print("=" * 60)
    print(f"  Pacientes  : {N_PATIENTS:,}")
    print(f"  Ventana    : {WINDOW_START} — {WINDOW_END}")
    print(f"  SEED       : {SEED}")
    print(f"  Salida     : {OUTPUT_DIR}/")
    print()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Construyendo población base...")
    patients = build_population()
    lc = {0: 0, 1: 0}
    for p in patients:
        lc[p["frailty_label"]] += 1
    print(f"  No frágil={lc[0]} ({lc[0]/N_PATIENTS:.1%})  "
          f"Frágil={lc[1]} ({lc[1]/N_PATIENTS:.1%})")
    n_trans = sum(1 for p in patients if p["transition_month"] is not None)
    print(f"  Pacientes con transicion (no fragil -> fragil): {n_trans}")
    print()

    print("Generando Fuente A (registros clínicos, 18 CSV mensuales)...")
    generate_source_a(patients, OUTPUT_DIR)
    print()

    print("Generando Fuente B1 (encuestas SPPB, 3 oleadas semestrales)...")
    generate_source_b1(patients, OUTPUT_DIR)
    print()

    print("Generando Fuente B2 (encuestas hábitos, 3 oleadas semestrales)...")
    generate_source_b2(patients, OUTPUT_DIR)
    print()

    print("Generando Fuente C (eventos de zancada, 18 JSONL mensuales)...")
    generate_source_c(patients, OUTPUT_DIR)
    print()

    run_verifications(patients, OUTPUT_DIR)

    print("\nFin.")


if __name__ == "__main__":
    main()
