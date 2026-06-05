def get_clinical_rules() -> list[dict]:
    """
    Reglas de calidad para la Fuente A (registros clínicos).
    Rangos derivados de los parámetros gauss_clamp del generador:
    límites más amplios entre los tres estados (robusto/prefrágil/frágil).
    """
    return [
        # Identidad
        {"name": "valid_patient_id",
         "constraint": "patient_id IS NOT NULL",
         "tag": "clinical"},
        {"name": "valid_snapshot_date",
         "constraint": "snapshot_date IS NOT NULL",
         "tag": "clinical"},

        # Demografía
        # _AGE_PARAMS:    min lo=65, max hi=95
        {"name": "valid_age",
         "constraint": "age >= 65 AND age <= 95",
         "tag": "clinical"},
        # weighted_choice(["M","F"])
        {"name": "valid_sex",
         "constraint": "sex IN ('M', 'F')",
         "tag": "clinical"},
        # _HEIGHT_PARAMS: lo=145, hi=190
        {"name": "valid_height",
         "constraint": "height_cm >= 145 AND height_cm <= 190",
         "tag": "clinical"},
        # _WEIGHT_PARAMS: min lo=40, max hi=105; _vary respeta límites
        {"name": "valid_weight",
         "constraint": "weight_kg >= 40 AND weight_kg <= 105",
         "tag": "clinical"},
        # Derivado: weight/height²; mín=40/1.90²≈11.08, máx=105/1.45²≈49.94
        {"name": "valid_bmi",
         "constraint": "bmi >= 11.0 AND bmi <= 50.0",
         "tag": "clinical"},

        # Clínica
        # _HR_PARAMS:   min lo=50, max hi=100
        {"name": "valid_heart_rate",
         "constraint": "heart_rate_bpm >= 50 AND heart_rate_bpm <= 100",
         "tag": "clinical"},
        # _TUG_PARAMS:  lo=5, hi=60
        {"name": "valid_tug",
         "constraint": "tug_time_s >= 5 AND tug_time_s <= 60",
         "tag": "clinical"},
        # _GRIP_PARAMS: lo=5, hi=50
        {"name": "valid_grip",
         "constraint": "grip_strength_kg >= 5 AND grip_strength_kg <= 50",
         "tag": "clinical"},
        # _MMSE_PARAMS: lo=10, hi=30
        {"name": "valid_mmse",
         "constraint": "mmse >= 10 AND mmse <= 30",
         "tag": "clinical"},
        # _GDS_PARAMS:  lo=1, hi=7
        {"name": "valid_gds",
         "constraint": "gds >= 1.0 AND gds <= 7.0",
         "tag": "clinical"},
        # _FI_PARAMS:   lo=0.0, hi=0.70
        {"name": "valid_frailty_index",
         "constraint": "frailty_index_fi >= 0.0 AND frailty_index_fi <= 0.70",
         "tag": "clinical"},

        # Criterios de Fried — bernoulli(p) ∈ {0, 1}
        {"name": "valid_fried_weight_loss",
         "constraint": "fried_weight_loss IN (0, 1)",
         "tag": "clinical"},
        {"name": "valid_fried_weakness",
         "constraint": "fried_weakness IN (0, 1)",
         "tag": "clinical"},
        {"name": "valid_fried_slowness",
         "constraint": "fried_slowness IN (0, 1)",
         "tag": "clinical"},
        {"name": "valid_fried_low_activity",
         "constraint": "fried_low_activity IN (0, 1)",
         "tag": "clinical"},
        {"name": "valid_fried_exhaustion",
         "constraint": "fried_exhaustion IN (0, 1)",
         "tag": "clinical"},

        # Etiqueta de salida
        {"name": "valid_frailty_label",
         "constraint": "frailty_label IN (0, 1)",
         "tag": "clinical"},

        # Consistencia: bmi = round(weight / (height/100)², 2)
        # El error máximo de redondeo con 2 decimales es < 0.005, se usa 0.01 de margen.
        {"name": "bmi_consistent",
         "constraint": "ABS(bmi - weight_kg / POW(height_cm / 100.0, 2)) < 0.01",
         "tag": "clinical"},
    ]
