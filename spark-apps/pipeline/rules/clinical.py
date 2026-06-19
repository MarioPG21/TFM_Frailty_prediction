def get_clinical_rules() -> list[dict]:
    """
    Reglas de calidad para la Fuente A (registros clínicos).
    Rangos derivados de los parámetros gauss_clamp del generador.
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
        {"name": "valid_age",
         "constraint": "age >= 65 AND age <= 100",
         "tag": "clinical"},
        {"name": "valid_sex",
         "constraint": "sex IN ('M', 'F')",
         "tag": "clinical"},
        # gauss_clamp(27/25, 4, 17, 45)
        {"name": "valid_bmi",
         "constraint": "bmi >= 17.0 AND bmi <= 45.0",
         "tag": "clinical"},

        # Clínica cardiovascular
        # gauss_clamp(140/130, 18, 90, 200)
        {"name": "valid_systolic_bp",
         "constraint": "systolic_bp >= 90 AND systolic_bp <= 200",
         "tag": "clinical"},
        # gauss_clamp(80, 10, 55, 120)
        {"name": "valid_diastolic_bp",
         "constraint": "diastolic_bp >= 55 AND diastolic_bp <= 120",
         "tag": "clinical"},

        # Laboratorio
        # gauss_clamp(55/72, 18, 15, 120)
        {"name": "valid_gfr",
         "constraint": "gfr >= 15.0 AND gfr <= 120.0",
         "tag": "clinical"},
        # gauss_clamp(3.5/4.0, 0.5, 2.0, 5.5)
        {"name": "valid_albumin",
         "constraint": "albumin >= 2.0 AND albumin <= 5.5",
         "tag": "clinical"},
        # gauss_clamp(12.5/14.0, 1.5, 8.0, 18.0)
        {"name": "valid_hemoglobin",
         "constraint": "hemoglobin >= 8.0 AND hemoglobin <= 18.0",
         "tag": "clinical"},

        # Comorbilidad y fármacos
        # max(0, gauss_clamp(3/1, 1.5, 0, 10))
        {"name": "valid_comorbidity_index",
         "constraint": "comorbidity_index >= 0 AND comorbidity_index <= 10",
         "tag": "clinical"},
        # max(0, gauss_clamp(5/2, 2, 0, 15))
        {"name": "valid_polypharmacy",
         "constraint": "polypharmacy >= 0 AND polypharmacy <= 15",
         "tag": "clinical"},

        # Eventos
        # 0 si no frágil; randint(1,4) si frágil
        {"name": "valid_falls_last_12m",
         "constraint": "falls_last_12m >= 0 AND falls_last_12m <= 4",
         "tag": "clinical"},
        # 0 si no frágil; randint(0,3) si frágil
        {"name": "valid_hospitalizations",
         "constraint": "hospitalizations_last_12m >= 0 AND hospitalizations_last_12m <= 3",
         "tag": "clinical"},

        # Cognitivo y estado de ánimo
        # gauss_clamp(22/27, 4, 0, 30)
        {"name": "valid_mmse_score",
         "constraint": "mmse_score >= 0 AND mmse_score <= 30",
         "tag": "clinical"},
        # uniform(0.3,1.0) si frágil; uniform(0.0,0.4) si no
        {"name": "valid_depression_score",
         "constraint": "depression_score >= 0.0 AND depression_score <= 1.0",
         "tag": "clinical"},
    ]
