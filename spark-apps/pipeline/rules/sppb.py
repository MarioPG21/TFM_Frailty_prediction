def get_sppb_rules() -> list[dict]:
    """
    Reglas de calidad para la Fuente B1 (encuestas SPPB y miedo a caídas).
    Rangos derivados de _SPPB_BALANCE, _SPPB_GAIT, _SPPB_CHAIR,
    _FES_PARAMS y _FALLS_PROBS del generador.
    """
    return [
        # Identidad
        {"name": "valid_response_id",
         "constraint": "response_id IS NOT NULL",
         "tag": "sppb"},
        {"name": "valid_patient_id_sppb",
         "constraint": "patient_id IS NOT NULL",
         "tag": "sppb"},
        {"name": "valid_survey_date_sppb",
         "constraint": "survey_date IS NOT NULL",
         "tag": "sppb"},

        # SPPB — weighted_choice([0..4])
        {"name": "valid_sppb_balance",
         "constraint": "sppb_balance >= 0 AND sppb_balance <= 4",
         "tag": "sppb"},
        {"name": "valid_sppb_gait_speed",
         "constraint": "sppb_gait_speed >= 0 AND sppb_gait_speed <= 4",
         "tag": "sppb"},
        {"name": "valid_sppb_chair_stand",
         "constraint": "sppb_chair_stand >= 0 AND sppb_chair_stand <= 4",
         "tag": "sppb"},
        {"name": "valid_sppb_total",
         "constraint": "sppb_total >= 0 AND sppb_total <= 12",
         "tag": "sppb"},
        # Invariante aritmético: el generador calcula sppb_total = bal + gait + chair
        {"name": "sppb_total_consistent",
         "constraint": "sppb_total = sppb_balance + sppb_gait_speed + sppb_chair_stand",
         "tag": "sppb"},

        # FES-I — _FES_PARAMS: min lo=7, max hi=28
        {"name": "valid_fes_i",
         "constraint": "fes_i_score >= 7.0 AND fes_i_score <= 28.0",
         "tag": "sppb"},

        # Caídas — bernoulli(p) ∈ {0, 1}
        {"name": "valid_falls",
         "constraint": "falls_last_year IN (0, 1)",
         "tag": "sppb"},
    ]
