def get_sppb_rules() -> list[dict]:
    """
    Reglas de calidad para la Fuente B1 (encuestas SPPB).
    Rangos derivados de los parámetros gauss_clamp del generador.
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

        # SPPB balance — gauss_clamp(2.5/3.8, 1.0, 0, 4)
        {"name": "valid_sppb_balance",
         "constraint": "sppb_balance >= 0.0 AND sppb_balance <= 4.0",
         "tag": "sppb"},
        # Velocidad de marcha (segundos brutos) — gauss_clamp(5.5/3.5, 1.5, 1, 15)
        {"name": "valid_sppb_gait_speed_s",
         "constraint": "sppb_gait_speed_s >= 1.0 AND sppb_gait_speed_s <= 15.0",
         "tag": "sppb"},
        # Tiempo levantarse (segundos brutos) — gauss_clamp(16/11, 4, 5, 60)
        {"name": "valid_sppb_chair_stand_s",
         "constraint": "sppb_chair_stand_s >= 5.0 AND sppb_chair_stand_s <= 60.0",
         "tag": "sppb"},
        # Puntuación total SPPB (0-12)
        {"name": "valid_sppb_total",
         "constraint": "sppb_total >= 0 AND sppb_total <= 12",
         "tag": "sppb"},
    ]
